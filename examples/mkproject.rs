//! Create a throwaway project owned by the test account; with OLB_HISTORY=1,
//! migrate it to history-ot. Prints the project id.
use overleaf_bridge::rawclient::Raw;
use serde_json::Value;

#[tokio::main]
async fn main() {
    let creds: Value = serde_json::from_str(&std::fs::read_to_string(".test-credentials").unwrap()).unwrap();
    let raw = Raw::login(creds["server"].as_str().unwrap(), creds["email"].as_str().unwrap(), creds["password"].as_str().unwrap()).await.unwrap();
    let name = format!("olb-probe-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
    let pid = raw.create_project(&name).await.unwrap();
    if std::env::var("OLB_HISTORY").is_ok() {
        let cmd = format!("docker exec mongo mongosh --quiet sharelatex --eval \"db.projects.updateOne({{_id: ObjectId('{pid}')}}, {{\\$max: {{'overleaf.history.otMigrationStage': 1}}}})\"");
        let out = std::process::Command::new("ssh").args(["yulee", &format!("bash -c '{}'", cmd.replace('\'', "'\\''"))]).output().unwrap();
        eprintln!("{}", String::from_utf8_lossy(&out.stdout).trim().replace('\n', " "));
    }
    println!("{pid}");
}
