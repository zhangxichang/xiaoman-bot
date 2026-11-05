mod agent;

use kovi::PluginBuilder;
use kovi::message::Message;
use kovi_onebot::EventRegistrar;

use crate::agent::{Agent, UserMessage};

#[kovi::plugin]
async fn main() {
    let agent = match Agent::new() {
        Ok(agent) => agent,
        Err(err) => {
            log::error!("{}", err);
            return;
        }
    };
    PluginBuilder::on_group_msg(move |event| {
        let agent = agent.clone();
        async move {
            if event.user_id == event.self_id {
                return;
            }
            let text = event.borrow_text().unwrap_or_default();
            let mentioned = is_mentioned(&event.message, event.self_id);
            if text.trim().is_empty() && !mentioned {
                return;
            }
            let group_id = event.group_id;
            let message = UserMessage {
                user_id: event.user_id,
                user_name: event.get_sender_nickname(),
                text: text.to_string(),
                mentioned,
            };
            if let Err(err) = agent.chat(event, group_id, message).await {
                log::error!("{}", err);
            }
        }
    });
}

fn is_mentioned(message: &Message, self_id: i64) -> bool {
    message.iter().any(|segment| {
        if segment.kind != "at" {
            return false;
        }
        match segment.data.get("qq") {
            Some(qq) => {
                qq.as_i64() == Some(self_id)
                    || qq.as_str().and_then(|qq| qq.parse::<i64>().ok()) == Some(self_id)
            }
            None => false,
        }
    })
}
