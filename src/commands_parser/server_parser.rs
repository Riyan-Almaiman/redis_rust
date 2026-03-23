use super::RedisCommand;

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "unsubscribe" => {
            if args.is_empty() {
                return Err("no params".into());
            }
            Ok(RedisCommand::Unsubscribe(args[0].to_owned()))
        }
        "publish" => {
            if args.len() < 2 {
                return Err("no params".into());
            }
            Ok(RedisCommand::Publish {
                channel: args[0].to_owned(),
                message: args[1].to_owned(),
            })
        }
        "subscribe" => {
            if args.is_empty() {
                return Err("no params".into());
            }
            Ok(RedisCommand::Subscribe(args[0].to_owned()))
        }
        "wait" => {
            if args.len() < 2 {
                return Err("WAIT requires numreplicas and timeout".into());
            }
            Ok(RedisCommand::Wait {
                replicas_num: args[0].parse::<u64>().map_err(|_| "Invalid numreplicas")?,
                timeout: args[1].parse::<u64>().map_err(|_| "Invalid timeout")?,
            })
        }
        "config" => {
            if args.is_empty() {
                return Err("no params".into());
            }
            Ok(RedisCommand::Config(
                args.iter().map(|arg| arg.to_string()).collect(),
            ))
        }
        "psync" => Ok(RedisCommand::PSYNC {
            replication_id: "?".to_string(),
            replication_offset: "-1".to_string(),
        }),
        "replconf" => Ok(RedisCommand::REPLCONF(
            args.iter().map(|arg| arg.to_string()).collect(),
        )),
        "info" => {
            if let Some(section) = args.first() {
                Ok(RedisCommand::Info {
                    section: Some((*section).to_string()),
                })
            } else {
                Ok(RedisCommand::Info { section: None })
            }
        }
        "exec" => Ok(RedisCommand::Exec),
        "discard" => Ok(RedisCommand::Discard),
        "ping" => Ok(RedisCommand::Ping),
        "multi" => Ok(RedisCommand::Multi),
        _ => Err(format!("Unknown command: {}", command)),
    }
}
