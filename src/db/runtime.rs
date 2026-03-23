use base64::Engine;
use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::mpsc;
use uuid::Uuid;


use crate::commands::CommandResult;
use crate::commands_parser::RedisCommand;
use crate::db::{ClientRequest, DB};
use crate::resp::Resp;
use crate::resp::Resp::{BulkString, Integer};
use crate::role::Role;
use crate::send::send_cmd;

impl DB {
    fn is_write_command(cmd: &RedisCommand) -> bool {
        matches!(
            cmd,
            RedisCommand::Set { .. }
                | RedisCommand::Incr { .. }
                | RedisCommand::RPush { .. }
                | RedisCommand::LPush { .. }
                | RedisCommand::LPop { .. }
                | RedisCommand::XAdd { .. }
        )
    }

    fn send_slaves(&mut self, resp: &Vec<u8>) -> u64 {
        self.role.increment_offset(resp.len());
        self.slaves
            .retain(|_id, slave| slave.send(resp.clone()).is_ok());
        self.slaves.len() as u64
    }

    fn cleanup_dead_slaves(&mut self) -> u64 {
        self.slaves.retain(|_id, slave| !slave.is_closed());
        self.slaves.len() as u64
    }

    fn handle_subscribed_client(
        &mut self,
        client_id: Uuid,
        command: &RedisCommand,
        response_tx: &mpsc::UnboundedSender<Vec<u8>>,
    ) -> bool {
        if !self.is_subscribed_client(client_id) {
            return false;
        }

        match command {
            RedisCommand::Ping => {
                send_cmd(
                    response_tx.clone(),
                    Resp::Array(VecDeque::from(vec![
                        BulkString(b"pong".to_vec()),
                        BulkString(b"".to_vec()),
                    ])),
                );
            }
            RedisCommand::Subscribe(channel) => {
                let count = self.subscribe_client(client_id, channel.clone());
                let mut resp = VecDeque::new();
                resp.push_back(BulkString(b"subscribe".to_vec()));
                resp.push_back(BulkString(channel.as_bytes().to_vec()));
                resp.push_back(Integer(count));
                self.set_subscriber_tx(client_id, response_tx.clone());

                send_cmd(response_tx.clone(), Resp::Array(resp));
            }
            RedisCommand::Unsubscribe(channel) => {
                let remaining = self.unsubscribe_client(client_id, channel);
                let response = Resp::Array(
                    vec![
                        Resp::BulkString(b"unsubscribe".to_vec()),
                        Resp::BulkString(channel.clone().into_bytes()),
                        Resp::Integer(remaining),
                    ]
                    .into(),
                );

                send_cmd(response_tx.clone(), response);
            }
            _ => {
                let res = format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    command.name()
                );
                send_cmd(response_tx.clone(), Resp::Error(res.into_bytes()));
            }
        }

        true
    }

    fn handle_multi_client(
        &mut self,
        client_id: Uuid,
        command: RedisCommand,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) -> bool {
        if !self.is_in_multi(client_id) {
            return false;
        }

        match command {
            RedisCommand::Exec => {
                let cmds = self.take_multi_commands(client_id).unwrap_or_default();
                let mut responses = VecDeque::new();

                for cmd in cmds {
                    if let CommandResult::Response(resp) = self.execute_commands(cmd, client_id) {
                        responses.push_back(resp);
                    }
                }
                send_cmd(response_tx, Resp::Array(responses));
            }
            RedisCommand::Discard => {
                self.discard_multi(client_id);
                send_cmd(response_tx, Resp::SimpleString(b"OK".to_vec()));
            }
            _ => {
                self.queue_command(client_id, command);
                send_cmd(response_tx, Resp::SimpleString(b"QUEUED".to_vec()));
            }
        }

        true
    }

    fn handle_wait_result(
        &mut self,
        timeout: u64,
        replicas: u64,
        offset: u64,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        self.cleanup_dead_slaves();
        let slave_count = self.slaves.len() as u64;

        if slave_count == 0 || offset == 0 {
            send_cmd(response_tx, Resp::Integer(slave_count as usize));
            return;
        }

        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<(Uuid, u64)>();
        self.ack_waiters.push(ack_tx);

        let mut ack_buf = Vec::new();
        Resp::Array(
            vec![
                Resp::BulkString(b"REPLCONF".to_vec()),
                Resp::BulkString(b"GETACK".to_vec()),
                Resp::BulkString(b"*".to_vec()),
            ]
            .into(),
        )
        .write_format(&mut ack_buf);

        let mut slaves = self.slaves.clone();

        tokio::spawn(async move {
            let mut acked = 0;
            let deadline = tokio::time::sleep(Duration::from_millis(timeout));
            tokio::pin!(deadline);
            let _interval = tokio::time::interval(Duration::from_millis(50));

            for slave in slaves.values() {
                let _ = slave.send(ack_buf.clone());
            }

            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    Some((slave_id, ack_offset)) = ack_rx.recv() => {
                        if ack_offset >= offset {
                            slaves.remove_entry(&slave_id);
                            acked += 1;
                        }
                        if acked as u64 >= replicas {
                            break;
                        }
                    }
                }
            }

            send_cmd(response_tx, Resp::Integer(acked));
        });
    }

    fn handle_register_slave(
        &mut self,
        client_id: Uuid,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
        resp: Resp,
    ) {
        match self.role {
            Role::Master { .. } => {
                send_cmd(response_tx.clone(), resp);

                let rdb_bytes = base64::engine::general_purpose::STANDARD
                    .decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
                    .unwrap();
                let header = format!("${}\r\n", rdb_bytes.len());
                let _ = response_tx.send(header.into_bytes());
                let _ = response_tx.send(rdb_bytes);

                self.slaves.insert(client_id, response_tx);
            }
            Role::Slave { .. } => {
                send_cmd(response_tx, resp);
            }
        }
    }

    fn handle_exec_result(
        &mut self,
        client_id: Uuid,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
        cmds: Vec<RedisCommand>,
    ) {
        let mut responses = VecDeque::new();
        for cmd in cmds {
            if let CommandResult::Response(resp) = self.execute_commands(cmd, client_id) {
                responses.push_back(resp);
            }
        }
        send_cmd(response_tx, Resp::Array(responses));
    }

    fn handle_command_result(
        &mut self,
        client_id: Uuid,
        response_tx: mpsc::UnboundedSender<Vec<u8>>,
        outcome: CommandResult,
    ) {
        match outcome {
            CommandResult::Subscribe(resp) => {
                self.set_subscriber_tx(client_id, response_tx.clone());
                send_cmd(response_tx, resp);
            }
            CommandResult::Response(resp) => {
                send_cmd(response_tx, resp);
            }
            CommandResult::Wait {
                timeout,
                replicas,
                offset,
            } => {
                self.handle_wait_result(timeout, replicas, offset, response_tx);
            }
            CommandResult::RegisterSlave(resp) => {
                self.handle_register_slave(client_id, response_tx, resp);
            }
            CommandResult::BlockList { keys, timeout } => {
                self.create_blocking_client(client_id, keys, response_tx, timeout);
            }
            CommandResult::BlockStream {
                client_id,
                streams,
                timeout_ms,
            } => {
                self.create_blocking_stream_client(client_id, streams, response_tx, timeout_ms);
            }
            CommandResult::Exec(cmds) => {
                self.handle_exec_result(client_id, response_tx, cmds);
            }
            CommandResult::None => {}
        }
    }

    pub async fn start(&mut self) {
        while let Some(request) = self.receiver.recv().await {
            let (command, response_tx, client_id, resp_command) = match request {
                ClientRequest::Connected {
                    client_id,
                    response_tx,
                } => {
                    self.initialize_session(client_id, response_tx);
                    continue;
                }
                ClientRequest::Command {
                    command,
                    response_tx,
                    client_id,
                    resp_command,
                } => (command, response_tx, client_id, resp_command),
            };
            let session = self.sessions.get(&client_id);
            if let Some(session) = session {
                    if session.authenticated_user.is_none() && !matches!(command, RedisCommand::Auth { .. } ) {
                        let res = b"NOAUTH Authentication required.".to_vec();
                        send_cmd(response_tx.clone(), Resp::Error(res));
                        continue;
                    }
            }
            else {
                self.initialize_session(client_id, response_tx);
                continue;;
            }

            if self.handle_subscribed_client(client_id, &command, &response_tx) {
                continue;
            }

            let mut cmd_buffer = Vec::new();
            resp_command.write_format(&mut cmd_buffer);

            if self.handle_multi_client(client_id, command.clone(), response_tx.clone()) {
                continue;
            }

            let outcome = self.execute_commands(command.clone(), client_id);
            self.handle_command_result(client_id, response_tx, outcome);

            if Self::is_write_command(&command) {
                let _slaves_count = self.send_slaves(&cmd_buffer);
            }
        }
    }
}