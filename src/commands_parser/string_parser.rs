use super::RedisCommand;

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "keys" => {
            if args.is_empty() {
                return Err("no params".into());
            }
            Ok(RedisCommand::Keys(args[0].to_owned()))
        }
        "incr" => {
            if args.is_empty() {
                return Err("INCR requires an argument".into());
            }
            Ok(RedisCommand::Incr {
                key: args[0].as_bytes().to_vec(),
            })
        }
        "echo" => {
            if args.is_empty() {
                return Err("ECHO requires an argument".into());
            }
            Ok(RedisCommand::Echo(args[0].as_bytes().to_vec()))
        }
        "set" => parse_set(args),
        "get" => {
            if args.is_empty() {
                return Err("GET requires a key".into());
            }
            Ok(RedisCommand::Get(args[0].as_bytes().to_vec()))
        }
        "type" => {
            if args.is_empty() {
                return Err("TYPE requires a key".into());
            }
            Ok(RedisCommand::Type(args[0].as_bytes().to_vec()))
        }
        _ => Err(format!("Unknown command: {}", command)),
    }
}

fn parse_set(args: &[&str]) -> Result<RedisCommand, String> {
    if args.len() < 2 {
        return Err("SET requires a key and a value".into());
    }

    let mut expiry = None;
    let mut index = 2;

    while index < args.len() {
        match args[index].to_ascii_lowercase().as_str() {
            "px" => {
                if index + 1 >= args.len() {
                    return Err("SET PX requires timeout".into());
                }
                expiry = Some(args[index + 1].parse::<u64>().map_err(|_| "Invalid PX")?);
                index += 2;
            }
            "ex" => {
                if index + 1 >= args.len() {
                    return Err("SET EX requires timeout".into());
                }
                let secs = args[index + 1].parse::<u64>().map_err(|_| "Invalid EX")?;
                expiry = Some(secs * 1000);
                index += 2;
            }
            _ => index += 1,
        }
    }

    Ok(RedisCommand::Set {
        key: args[0].as_bytes().to_vec(),
        value: args[1].as_bytes().to_vec(),
        expiry,
    })
}