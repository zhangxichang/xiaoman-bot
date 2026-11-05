use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    kovi::build_bot!(kovi_onebot::OneBotDriver::new(kovi_onebot::load_local_conf()?); basic)
        .run()
        .await;
    Ok(())
}
