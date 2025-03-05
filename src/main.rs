use csv::Reader;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::process::exit;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::Instant;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(PartialEq, Debug)]
enum TransactionState {
    None,
    Dispute,
    Resolve,
    Chargeback,
}
impl Default for TransactionState {
    fn default() -> Self {
        TransactionState::None
    }
}

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "type")]
    kind: TransactionType,
    client: u16,
    tx: u32,
    amount: f64,

    #[serde(skip)]
    state: TransactionState,
}

#[derive(Debug, Serialize)]
struct Client {
    client: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

struct PaymentEngine {
    clients: HashMap<u16, Client>,
    executed_transactions: HashMap<u32, Transaction>,
}

impl Default for PaymentEngine {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
            executed_transactions: HashMap::new(),
        }
    }
}

impl PaymentEngine {
    fn process_transaction(&mut self, transaction: Transaction) {
        let client = self.clients.entry(transaction.client).or_insert(Client {
            client: transaction.client,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        });

        if client.locked || transaction.amount < 0.0 {
            return;
        }

        match transaction.kind {
            TransactionType::Deposit => {
                client.available += transaction.amount;
                client.total += transaction.amount;

                self.executed_transactions
                    .insert(transaction.tx, transaction);
            }
            TransactionType::Withdrawal => {
                if client.available >= transaction.amount {
                    client.available -= transaction.amount;
                    client.total -= transaction.amount;

                    self.executed_transactions
                        .insert(transaction.tx, transaction);
                }
            }
            TransactionType::Dispute => {
                if let Some(reference_transaction) =
                    self.executed_transactions.get_mut(&transaction.tx)
                {
                    if reference_transaction.client != transaction.client {
                        // client is trying to dispute a transaction that does not belong to them
                        return;
                    }

                    match (&reference_transaction.state, &reference_transaction.kind) {
                        (
                            TransactionState::None,
                            TransactionType::Deposit | TransactionType::Withdrawal,
                        ) => {
                            client.held += reference_transaction.amount;
                            client.available -= reference_transaction.amount;
                            reference_transaction.state = TransactionState::Dispute;
                        }
                        _ => {}
                    }
                }
            }
            TransactionType::Resolve => {
                if let Some(reference_transaction) =
                    self.executed_transactions.get_mut(&transaction.tx)
                {
                    if reference_transaction.state == TransactionState::Dispute {
                        client.held -= reference_transaction.amount;
                        client.available += reference_transaction.amount;
                        reference_transaction.state = TransactionState::Resolve;
                    }
                }
            }
            TransactionType::Chargeback => {
                if let Some(reference_transaction) =
                    self.executed_transactions.get_mut(&transaction.tx)
                {
                    if reference_transaction.state == TransactionState::Dispute {
                        client.held -= reference_transaction.amount;
                        client.total -= reference_transaction.amount;
                        client.locked = true;
                        reference_transaction.state = TransactionState::Chargeback;
                    }
                }
            }
        }
    }

    fn process_transactions(&mut self, rx: Receiver<Transaction>) {
        while let Ok(transaction) = rx.recv() {
            self.process_transaction(transaction);
        }
    }

    fn read_input(reader: &mut Reader<File>, tx: Sender<Transaction>) {
        for result in reader.deserialize() {
            match result {
                Ok(record) => {
                    tx.send(record).expect("Failed to send transaction.");
                }
                Err(_) => {}
            }
        }
    }

    fn start(&mut self, path: &str) -> Result<(), Box<dyn Error>> {
        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_path(path)?;

        let (tx, rx) = std::sync::mpsc::channel();

        thread::scope(|scope| {
            let process_handle = scope.spawn(|| self.process_transactions(rx));
            let read_handle = scope.spawn(|| Self::read_input(&mut reader, tx));

            read_handle.join().unwrap();
            process_handle.join().unwrap();
        });

        Ok(())
    }

    fn save_output(self) -> Result<(), Box<dyn Error>> {
        let mut writer = csv::WriterBuilder::new().from_writer(std::io::stdout());
        for client in self.clients.values() {
            writer.serialize(client)?;
        }
        writer.flush()?;
        Ok(())
    }
}

fn app() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <input_file>", args[0]);
        exit(1)
    }

    let mut payment_engine = PaymentEngine::default();
    payment_engine.start(&args[1])?;
    payment_engine.save_output()?;

    Ok(())
}

fn main() {
    let now = Instant::now();
    if let Err(err) = app() {
        println!("ERROR: {}", err);
        exit(1);
    }
    let elapsed = now.elapsed();
    _ = elapsed;
    // eprintln!("Elapsed time: {}ms", elapsed.as_millis());
}

#[cfg(test)]
mod tests {
    use crate::{Client, PaymentEngine, Transaction, TransactionState, TransactionType};
    use std::collections::VecDeque;

    #[test]
    fn test_deposit() {
        let mut payment_engine = PaymentEngine::default();

        let tx = Transaction {
            kind: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: 5.0,
            state: TransactionState::None,
        };

        assert_eq!(payment_engine.clients.contains_key(&tx.client), false);

        payment_engine.process_transaction(tx);

        let client = payment_engine.clients.get(&1).expect("Client not found");

        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);
    }

    #[test]
    fn test_withdraw() {
        let mut payment_engine = PaymentEngine::default();
        payment_engine.clients.insert(
            1,
            Client {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: false,
            },
        );

        let tx = Transaction {
            kind: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: 5.0,
            state: TransactionState::None,
        };

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);

        payment_engine.process_transaction(tx);

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 0.0);
        assert_eq!(client.total, 0.0);
    }

    #[test]
    fn test_withdraw_negative() {
        let mut payment_engine = PaymentEngine::default();
        payment_engine.clients.insert(
            1,
            Client {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: false,
            },
        );

        let tx = Transaction {
            kind: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: 10.0,
            state: TransactionState::None,
        };

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);

        payment_engine.process_transaction(tx);

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);
    }

    #[test]
    fn test_withdraw_locked() {
        let mut payment_engine = PaymentEngine::default();
        payment_engine.clients.insert(
            1,
            Client {
                client: 1,
                available: 5.0,
                held: 0.0,
                total: 5.0,
                locked: true,
            },
        );

        let tx = Transaction {
            kind: TransactionType::Withdrawal,
            client: 1,
            tx: 1,
            amount: 5.0,
            state: TransactionState::None,
        };

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);

        payment_engine.process_transaction(tx);

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);
    }

    #[test]
    fn test_chargeback() {
        let mut payment_engine = PaymentEngine::default();

        let mut transactions = VecDeque::from(vec![
            Transaction {
                kind: TransactionType::Deposit,
                client: 1,
                tx: 1,
                amount: 5.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Withdrawal,
                client: 1,
                tx: 2,
                amount: 5.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Dispute,
                client: 1,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Chargeback,
                client: 1,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
        ]);

        payment_engine.process_transaction(transactions.pop_front().unwrap());

        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.total, 5.0);

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 0.0);
        assert_eq!(client.total, 0.0);

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, -5.0);
        assert_eq!(client.held, 5.0);
        assert_eq!(client.total, 0.0);
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must be available")
                .state,
            TransactionState::Dispute
        );

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, -5.0);
        assert_eq!(client.held, 0.0);
        assert_eq!(client.total, -5.0);
        assert_eq!(client.locked, true);
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must be available")
                .state,
            TransactionState::Chargeback
        );
    }

    #[test]
    fn test_transaction_invalid_dispute_state() {
        let mut payment_engine = PaymentEngine::default();

        let mut transactions = VecDeque::from(vec![
            Transaction {
                kind: TransactionType::Deposit,
                client: 1,
                tx: 1,
                amount: 5.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Resolve,
                client: 1,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Dispute,
                client: 1,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Resolve,
                client: 1,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Dispute,
                client: 1,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
        ]);

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        payment_engine.process_transaction(transactions.pop_front().unwrap());
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must been executed")
                .state,
            TransactionState::None
        );

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must been executed")
                .state,
            TransactionState::Dispute
        );

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must been executed")
                .state,
            TransactionState::Resolve
        );

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must been executed")
                .state,
            TransactionState::Resolve
        );
    }

    #[test]
    fn test_dispute_invalid() {
        let mut payment_engine = PaymentEngine::default();

        let mut transactions = VecDeque::from(vec![
            Transaction {
                kind: TransactionType::Deposit,
                client: 1,
                tx: 1,
                amount: 5.0,
                state: TransactionState::None,
            },
            Transaction {
                kind: TransactionType::Dispute,
                client: 2,
                tx: 1,
                amount: 0.0,
                state: TransactionState::None,
            },
        ]);

        payment_engine.process_transaction(transactions.pop_front().unwrap());
        let client = payment_engine.clients.get(&1).expect("Client not found");
        assert_eq!(client.available, 5.0);
        assert_eq!(client.held, 0.0);
        assert_eq!(client.total, 5.0);
        assert_eq!(client.locked, false);
        assert_eq!(
            payment_engine
                .executed_transactions
                .get(&1)
                .expect("must be available")
                .state,
            TransactionState::None
        );
    }
}
