mod blocks;
mod config;
mod metrics;
mod transactions;

use crate::blocks::{BlockRow, BlockWriter};
use crate::config::GeyserPluginConfig;
use crate::metrics::Metrics;
use crate::transactions::{TransactionMessage, TransactionRow, TransactionWriter};
use anyhow::{Error as AnyhowError, Result as AnyhowResult};
use colored::Colorize;
use prometheus::IntCounter;
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc::Sender, Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug)]
struct ConfigError(String);

impl std::error::Error for ConfigError {}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    info!("Creating a new instance of GeyserPluginClickhouse");
    // Create a new instance of the plugin
    Box::into_raw(Box::new(GeyserPluginClickhouse::new()))
}

#[derive(Debug)]
pub struct GeyserPluginClickhouse {
    block_writer: Option<Arc<Mutex<BlockWriter>>>,
    transaction_writer: Option<Arc<Mutex<TransactionWriter>>>,
    clickhouse_config: Option<GeyserPluginConfig>,
    transaction_sender: Option<Sender<TransactionMessage>>,
    block_sender: Option<Sender<BlockRow>>,
    shutdown_flag: Option<Arc<AtomicBool>>,

    // Metrics
    processed_transactions: IntCounter,
    processed_blocks: IntCounter,
    transaction_errors: IntCounter,
    block_errors: IntCounter,
}

impl GeyserPluginClickhouse {
    fn new() -> Self {
        // Initialize metrics
        let processed_transactions =
            IntCounter::new("processed_transactions", "Number of transactions processed").unwrap();
        let processed_blocks =
            IntCounter::new("processed_blocks", "Number of blocks processed").unwrap();
        let transaction_errors = IntCounter::new(
            "transaction_errors",
            "Number of transaction processing errors",
        )
        .unwrap();
        let block_errors =
            IntCounter::new("block_errors", "Number of block processing errors").unwrap();

        // Register metrics with the default Prometheus registry
        let registry = prometheus::default_registry();
        registry
            .register(Box::new(processed_transactions.clone()))
            .unwrap();
        registry
            .register(Box::new(processed_blocks.clone()))
            .unwrap();
        registry
            .register(Box::new(transaction_errors.clone()))
            .unwrap();
        registry.register(Box::new(block_errors.clone())).unwrap();

        Self {
            clickhouse_config: None,
            block_writer: None,
            transaction_writer: None,
            transaction_sender: None,
            block_sender: None,
            shutdown_flag: None,
            processed_transactions,
            processed_blocks,
            transaction_errors,
            block_errors,
        }
    }

    fn convert_error(err: AnyhowError) -> GeyserPluginError {
        error!("Conversion error: {}", err);
        GeyserPluginError::Custom(Box::new(ConfigError(err.to_string())))
    }

    fn initialize(&mut self, config_file: &str) -> AnyhowResult<()> {
        info!("Initializing plugin with config file: {}", config_file);

        // Initialize tracing subscriber for logging
        let subscriber = FmtSubscriber::builder()
            .with_env_filter("solana=info,my_geyser_plugin=debug")
            .with_writer(std::io::stdout)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber");

        // Load and parse the configuration file
        let config = GeyserPluginConfig::from_file(config_file)?;
        info!("Configuration loaded successfully: {:?}", config);

        let clickhouse_config = config.clickhouse.clone();
        self.clickhouse_config = Some(config);

        // Initialize BlockWriter and TransactionWriter, propagating errors
        let block_writer = BlockWriter::new(clickhouse_config.clone())?;
        let block_writer = Arc::new(Mutex::new(block_writer));
        info!("BlockWriter initialized");

        let transaction_writer = TransactionWriter::new(clickhouse_config.clone())?;
        let transaction_writer = Arc::new(Mutex::new(transaction_writer));
        info!("TransactionWriter initialized");

        // Create synchronous channels
        let (transaction_sender, transaction_receiver) =
            std::sync::mpsc::channel::<TransactionMessage>();
        let (block_sender, block_receiver) = std::sync::mpsc::channel::<BlockRow>();
        self.transaction_sender = Some(transaction_sender);
        self.block_sender = Some(block_sender);

        // Initialize shutdown flag
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        self.shutdown_flag = Some(shutdown_flag.clone());

        // Clone necessary Arcs for worker threads
        let transaction_writer_clone = Arc::clone(&transaction_writer);
        let processed_transactions_clone = self.processed_transactions.clone();
        let transaction_errors_clone = self.transaction_errors.clone();

        // Spawn Transaction Worker Thread
        {
            let transaction_receiver = transaction_receiver;
            let shutdown_flag_tx = shutdown_flag.clone();
            thread::spawn(move || {
                // Create a new Tokio runtime for the worker thread
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create Tokio runtime for transaction processor");

                rt.block_on(async {
                    info!("Starting transaction processor thread");
                    loop {
                        if shutdown_flag_tx.load(Ordering::Relaxed) {
                            info!("Transaction processor received shutdown signal.");
                            break;
                        }

                        match transaction_receiver.recv_timeout(Duration::from_millis(100)) {
                            Ok(message) => {
                                debug!("Received transaction message over channel: {:?}", message);
                                let writer = transaction_writer_clone.lock().unwrap();
                                if let Err(e) = writer.add_transaction(message).await {
                                    error!("Error processing and inserting transaction into clickhouse: {:?}", e);
                                    transaction_errors_clone.inc();
                                } else {
                                    debug!("Transaction processed and sent to clickhouse successfully");
                                    processed_transactions_clone.inc();
                                }
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                // Timeout occurred, continue loop to check shutdown flag
                                continue;
                            }
                            Err(e) => {
                                error!("Transaction receiver encountered an error: {:?}", e);
                                break;
                            }
                        }
                    }
                    info!("Transaction processor thread exiting");
                });
            });
        }

        // Spawn Block Worker Thread
        {
            let block_receiver = block_receiver;
            let shutdown_flag_block = shutdown_flag.clone();
            let block_writer_clone = Arc::clone(&block_writer);
            let processed_blocks_clone = self.processed_blocks.clone();
            let block_errors_clone = self.block_errors.clone();

            thread::spawn(move || {
                // Create a new Tokio runtime for the worker thread
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create Tokio runtime for block processor");

                rt.block_on(async {
                    info!("Starting block processor thread");
                    loop {
                        if shutdown_flag_block.load(Ordering::Relaxed) {
                            info!("Block processor received shutdown signal.");
                            break;
                        }

                        match block_receiver.recv_timeout(Duration::from_millis(100)) {
                            Ok(block_row) => { // Received a fully populated BlockRow

                                let mut writer = block_writer_clone.lock().unwrap();
                                debug!("Received block message over channel: {:?}", block_row);
                                let clone = block_row.clone();
                                if let Err(e) = writer.add_block(block_row).await {
                                    error!("Error processing and inserting block into ClickHouse: {:?}", e);
                                    error!("Block row: {:?}", clone);
                                    block_errors_clone.inc();
                                } else {
                                    debug!("Block processed and sent to ClickHouse successfully: {:?}", clone);
                                    processed_blocks_clone.inc();
                                }
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                // Timeout occurred, continue loop to check shutdown flag
                                continue;
                            }
                            Err(e) => {
                                error!("Block receiver encountered an error: {:?}", e);
                                break;
                            }
                        }
                    }
                    info!("Block processor thread exiting");
                });
            });
        }

        // Initialize and serve metrics
        let metrics = Metrics::new();
        info!("Serving metrics on port 9898");
        metrics.serve(9898);

        // Assign writers to self
        self.block_writer = Some(block_writer);
        self.transaction_writer = Some(transaction_writer);

        info!("Plugin initialized successfully");
        Ok(())
    }
}

impl GeyserPlugin for GeyserPluginClickhouse {
    fn name(&self) -> &'static str {
        "plugin::GeyserPluginClickhouse"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        info!(
            "{} loading plugin: {:?} from config_file {:?}",
            "::geyser-plugin-clickhouse::".green().on_black(),
            self.name(),
            config_file
        );

        self.initialize(config_file).map_err(Self::convert_error)?;

        Ok(())
    }

    fn on_unload(&mut self) {
        // Set the shutdown flag
        if let Some(shutdown_flag) = &self.shutdown_flag {
            shutdown_flag.store(true, Ordering::Relaxed);
            info!("Shutdown flag set to true.");
        }

        info!(
            "{} unloading plugin: {:?}",
            "::geyser-plugin-clickhouse::".green().on_black(),
            self.name()
        );
    }

    fn notify_end_of_startup(&self) -> Result<()> {
        info!(
            "{} notifying the end of startup for accounts notifications",
            "::geyser-plugin-clickhouse::".green().on_black(),
        );
        Ok(())
    }

    /// Check if the plugin is interested in account data
    /// Default is true -- if the plugin is not interested in
    /// account data, return false.
    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    /// Check if the plugin is interested in block data
    fn entry_notifications_enabled(&self) -> bool {
        true
    }

    fn update_account(
        &self,
        _account: ReplicaAccountInfoVersions,
        _slot: u64,
        _is_startup: bool,
    ) -> Result<()> {
        // No action needed as account_data_notifications_enabled returns false
        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> Result<()> {
        // Add logging if needed
        debug!("notify_entry called");
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction_info: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> Result<()> {
        if let Some(sender) = &self.transaction_sender {
            // Ensure that `transaction_row` owns all its data
            let transaction_row = TransactionRow::from_replica(transaction_info)
                .map_err(|e| Self::convert_error(e.into()))?;
            debug!(
                "Converted transaction_info to TransactionRow: {:?}",
                transaction_row
            );

            // Create `transaction_message` with owned data
            let transaction_message = TransactionMessage {
                transaction: transaction_row,
                slot,
            };
            debug!("Created TransactionMessage: {:?}", transaction_message);

            let tx_hash = &transaction_message.transaction.signature;

            // Attempt to send the message synchronously
            match sender.send(transaction_message.clone()) {
                Ok(_) => {
                    debug!("Transaction {} sent over channel successfully", tx_hash);
                }
                Err(e) => {
                    error!("Failed to send transaction message over channel: {:?}", e);
                }
            }
        } else {
            error!("Transaction sender is not initialized");
        }
        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions<'_>) -> Result<()> {
        if let Some(sender) = &self.block_sender {
            // Convert ReplicaBlockInfoVersions to a fully populated BlockRow
            let message =
                BlockRow::from_replica(block_info).map_err(|e| Self::convert_error(e.into()))?;
            debug!("Converted block_info to BlockRow: {:?}", message);

            // Attempt to send the message synchronously
            let bh = message.block_height;
            match sender.send(message) {
                // Send the fully populated BlockRow
                Ok(_) => {
                    debug!("Block {} sent over channel successfully", bh);
                }
                Err(e) => {
                    error!("Failed to send block message: {:?}", e);
                }
            }
        } else {
            error!("Block sender is not initialized");
        }
        Ok(())
    }
}
