use super::RedisCommand;

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "rpush" => {
            if args.len() < 2 {
                return Err("RPUSH requires key and at least one element".into());
            }

            Ok(RedisCommand::RPush {
                key: args[0].as_bytes().to_vec(),
                elements: args[1..].iter().map(|arg| arg.as_bytes().to_vec()).collect(),
            })
        }
        "lpush" => {
            if args.len() < 2 {
                return Err("LPUSH requires key and at least one element".into());
            }

            Ok(RedisCommand::LPush {
                key: args[0].as_bytes().to_vec(),
                elements: args[1..].iter().map(|arg| arg.as_bytes().to_vec()).collect(),
            })
        }
        "lrange" => {
            if args.len() < 3 {
                return Err("LRANGE requires key, start, stop".into());
            }

            let start = args[1].parse::<i64>().map_err(|_| "Invalid start")?;
            let stop = args[2].parse::<i64>().map_err(|_| "Invalid stop")?;

            Ok(RedisCommand::LRange {
                key: args[0].as_bytes().to_vec(),
                start,
                stop,
            })
        }
        "llen" => {
            if args.is_empty() {
                return Err("LLEN requires a key".into());
            }

            Ok(RedisCommand::LLen(args[0].as_bytes().to_vec()))
        }
        "lpop" => {
            if args.is_empty() {
                return Err("LPOP requires a key".into());
            }

            let count = if args.len() >= 2 {
                args[1].parse::<usize>().map_err(|_| "Invalid count")?
            } else {
                1
            };

            Ok(RedisCommand::LPop {
                key: args[0].as_bytes().to_vec(),
                count,
            })
        }
        "blpop" => {
            if args.len() < 2 {
                return Err("BLPOP requires keys and timeout".into());
            }

            let timeout = args
                .last()
                .ok_or("BLPOP requires keys and timeout")?
                .parse::<f64>()
                .map_err(|_| "Invalid timeout")?;

            let keys = args[..args.len() - 1]
                .iter()
                .map(|arg| arg.as_bytes().to_vec())
                .collect();

            Ok(RedisCommand::BLPop { keys, timeout })
        }
        _ => Err(format!("Unknown command: {}", command)),
    }
}