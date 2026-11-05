use std::{
    collections::{HashMap, VecDeque},
    future::IntoFuture,
    sync::Arc,
};

use eyre::{Result, eyre};
use kovi_onebot::event::GroupMsgEvent;
use parking_lot::Mutex;
use rig::{AgentBuilder, completion::GenerationOptions, message::Message, providers::opencode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const MAX_HISTORY: usize = 50;
const CONCURRENCY_LIMIT: usize = 32;

#[derive(JsonSchema, Deserialize)]
pub struct ReplyDecision {
    /// 回复内容，为空则不回复
    #[serde(default)]
    pub reply: Option<String>,
}
impl ReplyDecision {
    fn schema() -> Result<String> {
        serde_json::to_string(&schemars::schema_for!(Self)).map_err(|err| eyre!("{}", err))
    }
}

#[derive(Serialize, JsonSchema)]
pub struct UserMessage {
    /// 用户id
    pub user_id: i64,
    /// 用户名
    pub user_name: String,
    /// 用户消息
    pub text: String,
    /// 是否 @ 了机器人（即使没有文字也会为 true）
    pub mentioned: bool,
}
impl UserMessage {
    fn schema() -> Result<String> {
        serde_json::to_string(&schemars::schema_for!(Self)).map_err(|err| eyre!("{}", err))
    }
    fn to_json(&self) -> String {
        serde_json::to_string(self).expect("serialize UserMessage")
    }
}

struct Job {
    event: Arc<GroupMsgEvent>,
    message: UserMessage,
}

#[derive(Clone)]
pub struct Agent {
    agent: rig::Agent,
    senders: Arc<Mutex<HashMap<i64, mpsc::Sender<Job>>>>,
}
impl Agent {
    pub fn new() -> Result<Self> {
        Ok(Self {
            agent: AgentBuilder::new(opencode::go_from_env()?.responses("deepseek-v4.1-flash"))
                .preamble(include_str!("../../../PREAMBLE.md"))
                .append_preamble(&format!(
                    "用户发给你的消息 JSON Schema:\n{}",
                    UserMessage::schema()?
                ))
                .append_preamble(&format!(
                    "你应该输出的结构 JSON Schema:\n{}",
                    ReplyDecision::schema()?
                ))
                .options(GenerationOptions::new().cache_key("xiaoman-bot"))
                .build(),
            senders: Default::default(),
        })
    }
    pub async fn chat(
        &self,
        event: Arc<GroupMsgEvent>,
        group_id: i64,
        message: UserMessage,
    ) -> Result<()> {
        let tx = self
            .senders
            .lock()
            .entry(group_id)
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(CONCURRENCY_LIMIT);
                tokio::spawn(worker(rx, self.agent.clone()));
                tx
            })
            .clone();
        tx.send(Job { event, message }).await?;
        Ok(())
    }
}

async fn worker(mut rx: mpsc::Receiver<Job>, agent: rig::Agent) {
    let mut history = VecDeque::<Message>::new();
    let mut backlog = Vec::<Job>::new();
    loop {
        let current_job = if let Some(job) = backlog.pop() {
            job
        } else if let Some(job) = rx.recv().await {
            job
        } else {
            break;
        };
        let mut current_history = history.clone();
        for job in &backlog {
            current_history.push_back(Message::user(job.message.to_json()));
        }
        let agent_request = agent
            .prompt_typed::<ReplyDecision>(current_job.message.to_json())
            .history(current_history)
            .into_future();
        tokio::select! {
            request_result = agent_request => {
                for job in &backlog {
                    history.push_back(Message::user(job.message.to_json()));
                }
                history.push_back(Message::user(current_job.message.to_json()));
                match request_result {
                    Ok(response) => {
                        if let Some(reply) = &response.output.reply {
                            current_job.event.reply(reply);
                            history.push_back(Message::assistant(reply));
                        }
                    }
                    Err(err) => log::error!("AI 请求失败: {}", err),
                }
                if history.len() > MAX_HISTORY {
                    history.drain(..history.len() - MAX_HISTORY);
                }
                backlog.clear();
            }
            incoming = rx.recv() => {
                match incoming {
                    Some(new_job) => {
                        backlog.push(current_job);
                        backlog.push(new_job);
                    }
                    None => break,
                }
            }
        }
    }
}
