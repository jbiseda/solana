use {
    crate::cli_output::CliSignatureVerificationStatus,
    chrono::{DateTime, Local, NaiveDateTime, SecondsFormat, TimeZone, Utc},
    console::style,
    indicatif::{ProgressBar, ProgressStyle},
    solana_sdk::{
        clock::UnixTimestamp,
        hash::Hash,
        instruction::CompiledInstruction,
        native_token::lamports_to_sol,
        program_utils::limited_deserialize,
        pubkey::Pubkey,
        signature::Signature,
        stake,
        transaction::{Transaction, TransactionError},
    },
    solana_transaction_status::{Rewards, UiTransactionStatusMeta},
    spl_memo::{id as spl_memo_id, v1::id as spl_memo_v1_id},
    std::{collections::HashMap, fmt, io},
};

#[derive(Clone, Debug)]
pub struct BuildBalanceMessageConfig {
    pub use_lamports_unit: bool,
    pub show_unit: bool,
    pub trim_trailing_zeros: bool,
}

impl Default for BuildBalanceMessageConfig {
    fn default() -> Self {
        Self {
            use_lamports_unit: false,
            show_unit: true,
            trim_trailing_zeros: true,
        }
    }
}

fn is_memo_program(k: &Pubkey) -> bool {
    let k_str = k.to_string();
    (k_str == spl_memo_v1_id().to_string()) || (k_str == spl_memo_id().to_string())
}

pub fn build_balance_message_with_config(
    lamports: u64,
    config: &BuildBalanceMessageConfig,
) -> String {
    let value = if config.use_lamports_unit {
        lamports.to_string()
    } else {
        let sol = lamports_to_sol(lamports);
        let sol_str = format!("{:.9}", sol);
        if config.trim_trailing_zeros {
            sol_str
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            sol_str
        }
    };
    let unit = if config.show_unit {
        if config.use_lamports_unit {
            let ess = if lamports == 1 { "" } else { "s" };
            format!(" lamport{}", ess)
        } else {
            " SOL".to_string()
        }
    } else {
        "".to_string()
    };
    format!("{}{}", value, unit)
}

pub fn build_balance_message(lamports: u64, use_lamports_unit: bool, show_unit: bool) -> String {
    build_balance_message_with_config(
        lamports,
        &BuildBalanceMessageConfig {
            use_lamports_unit,
            show_unit,
            ..BuildBalanceMessageConfig::default()
        },
    )
}

// Pretty print a "name value"
pub fn println_name_value(name: &str, value: &str) {
    let styled_value = if value.is_empty() {
        style("(not set)").italic()
    } else {
        style(value)
    };
    println!("{} {}", style(name).bold(), styled_value);
}

pub fn writeln_name_value(f: &mut dyn fmt::Write, name: &str, value: &str) -> fmt::Result {
    let styled_value = if value.is_empty() {
        style("(not set)").italic()
    } else {
        style(value)
    };
    writeln!(f, "{} {}", style(name).bold(), styled_value)
}

pub fn format_labeled_address(pubkey: &str, address_labels: &HashMap<String, String>) -> String {
    let label = address_labels.get(pubkey);
    match label {
        Some(label) => format!(
            "{:.31} ({:.4}..{})",
            label,
            pubkey,
            pubkey.split_at(pubkey.len() - 4).1
        ),
        None => pubkey.to_string(),
    }
}

pub fn println_signers(
    blockhash: &Hash,
    signers: &[String],
    absent: &[String],
    bad_sig: &[String],
) {
    println!();
    println!("Blockhash: {}", blockhash);
    if !signers.is_empty() {
        println!("Signers (Pubkey=Signature):");
        signers.iter().for_each(|signer| println!("  {}", signer))
    }
    if !absent.is_empty() {
        println!("Absent Signers (Pubkey):");
        absent.iter().for_each(|pubkey| println!("  {}", pubkey))
    }
    if !bad_sig.is_empty() {
        println!("Bad Signatures (Pubkey):");
        bad_sig.iter().for_each(|pubkey| println!("  {}", pubkey))
    }
    println!();
}

struct CliAccountMeta {
    is_signer: bool,
    is_writable: bool,
    is_invoked: bool,
}

fn format_account_mode(meta: CliAccountMeta) -> String {
    format!(
        "{}r{}{}", // accounts are always readable...
        if meta.is_signer {
            "s" // stands for signer
        } else {
            "-"
        },
        if meta.is_writable {
            "w" // comment for consistent rust fmt (no joking; lol)
        } else {
            "-"
        },
        // account may be executable on-chain while not being
        // designated as a program-id in the message
        if meta.is_invoked {
            "x"
        } else {
            // programs to be executed via CPI cannot be identified as
            // executable from the message
            "-"
        },
    )
}

fn write_transaction<W: io::Write>(
    w: &mut W,
    transaction: &Transaction,
    transaction_status: Option<&UiTransactionStatusMeta>,
    prefix: &str,
    sigverify_status: Option<&[CliSignatureVerificationStatus]>,
    block_time: Option<UnixTimestamp>,
    timezone: CliTimezone,
) -> io::Result<()> {
    write_block_time(w, block_time, timezone, prefix)?;

    let message = &transaction.message;
    write_recent_blockhash(w, &message.recent_blockhash, prefix)?;
    write_signatures(w, &transaction.signatures, sigverify_status, prefix)?;

    let mut fee_payer_index = None;
    for (account_index, account) in message.account_keys.iter().enumerate() {
        if fee_payer_index.is_none() && message.is_non_loader_key(account_index) {
            fee_payer_index = Some(account_index)
        }

        let account_meta = CliAccountMeta {
            is_signer: message.is_signer(account_index),
            is_writable: message.is_writable(account_index),
            is_invoked: message.maybe_executable(account_index),
        };

        write_account(
            w,
            account_index,
            account,
            format_account_mode(account_meta),
            Some(account_index) == fee_payer_index,
            prefix,
        )?;
    }

    for (instruction_index, instruction) in message.instructions.iter().enumerate() {
        let program_pubkey = message.account_keys[instruction.program_id_index as usize];
        let instruction_accounts = instruction.accounts.iter().map(|account_index| {
            let account_pubkey = &message.account_keys[*account_index as usize];
            (account_pubkey, *account_index)
        });

        write_instruction(
            w,
            instruction_index,
            &program_pubkey,
            instruction,
            instruction_accounts,
            prefix,
        )?;
    }

    if let Some(transaction_status) = transaction_status {
        write_status(w, &transaction_status.status, prefix)?;
        write_fees(w, transaction_status.fee, prefix)?;
        write_balances(w, transaction_status, prefix)?;
        write_log_messages(w, transaction_status.log_messages.as_ref(), prefix)?;
        write_rewards(w, transaction_status.rewards.as_ref(), prefix)?;
    } else {
        writeln!(w, "{}Status: Unavailable", prefix)?;
    }

    Ok(())
}

enum CliTimezone {
    Local,
    #[allow(dead_code)]
    Utc,
}

fn write_block_time<W: io::Write>(
    w: &mut W,
    block_time: Option<UnixTimestamp>,
    timezone: CliTimezone,
    prefix: &str,
) -> io::Result<()> {
    if let Some(block_time) = block_time {
        let block_time_output = match timezone {
            CliTimezone::Local => format!("{:?}", Local.timestamp(block_time, 0)),
            CliTimezone::Utc => format!("{:?}", Utc.timestamp(block_time, 0)),
        };
        writeln!(w, "{}Block Time: {}", prefix, block_time_output,)?;
    }
    Ok(())
}

fn write_recent_blockhash<W: io::Write>(
    w: &mut W,
    recent_blockhash: &Hash,
    prefix: &str,
) -> io::Result<()> {
    writeln!(w, "{}Recent Blockhash: {:?}", prefix, recent_blockhash)
}

fn write_signatures<W: io::Write>(
    w: &mut W,
    signatures: &[Signature],
    sigverify_status: Option<&[CliSignatureVerificationStatus]>,
    prefix: &str,
) -> io::Result<()> {
    let sigverify_statuses = if let Some(sigverify_status) = sigverify_status {
        sigverify_status
            .iter()
            .map(|s| format!(" ({})", s))
            .collect()
    } else {
        vec!["".to_string(); signatures.len()]
    };
    for (signature_index, (signature, sigverify_status)) in
        signatures.iter().zip(&sigverify_statuses).enumerate()
    {
        writeln!(
            w,
            "{}Signature {}: {:?}{}",
            prefix, signature_index, signature, sigverify_status,
        )?;
    }
    Ok(())
}

fn write_account<W: io::Write>(
    w: &mut W,
    account_index: usize,
    account_address: &Pubkey,
    account_mode: String,
    is_fee_payer: bool,
    prefix: &str,
) -> io::Result<()> {
    writeln!(
        w,
        "{}Account {}: {} {}{}",
        prefix,
        account_index,
        account_mode,
        account_address,
        if is_fee_payer { " (fee payer)" } else { "" },
    )
}

fn write_instruction<'a, W: io::Write>(
    w: &mut W,
    instruction_index: usize,
    program_pubkey: &Pubkey,
    instruction: &CompiledInstruction,
    instruction_accounts: impl Iterator<Item = (&'a Pubkey, u8)>,
    prefix: &str,
) -> io::Result<()> {
    writeln!(w, "{}Instruction {}", prefix, instruction_index)?;
    writeln!(
        w,
        "{}  Program:   {} ({})",
        prefix, program_pubkey, instruction.program_id_index
    )?;
    for (index, (account_address, account_index)) in instruction_accounts.enumerate() {
        writeln!(
            w,
            "{}  Account {}: {} ({})",
            prefix, index, account_address, account_index
        )?;
    }

    let mut raw = true;
    if program_pubkey == &solana_vote_program::id() {
        if let Ok(vote_instruction) = limited_deserialize::<
            solana_vote_program::vote_instruction::VoteInstruction,
        >(&instruction.data)
        {
            writeln!(w, "{}  {:?}", prefix, vote_instruction)?;
            raw = false;
        }
    } else if program_pubkey == &stake::program::id() {
        if let Ok(stake_instruction) =
            limited_deserialize::<stake::instruction::StakeInstruction>(&instruction.data)
        {
            writeln!(w, "{}  {:?}", prefix, stake_instruction)?;
            raw = false;
        }
    } else if program_pubkey == &solana_sdk::system_program::id() {
        if let Ok(system_instruction) = limited_deserialize::<
            solana_sdk::system_instruction::SystemInstruction,
        >(&instruction.data)
        {
            writeln!(w, "{}  {:?}", prefix, system_instruction)?;
            raw = false;
        }
    } else if is_memo_program(program_pubkey) {
        if let Ok(s) = std::str::from_utf8(&instruction.data) {
            writeln!(w, "{}  Data: \"{}\"", prefix, s)?;
            raw = false;
        }
    }

    if raw {
        writeln!(w, "{}  Data: {:?}", prefix, instruction.data)?;
    }

    Ok(())
}

fn write_rewards<W: io::Write>(
    w: &mut W,
    rewards: Option<&Rewards>,
    prefix: &str,
) -> io::Result<()> {
    if let Some(rewards) = rewards {
        if !rewards.is_empty() {
            writeln!(w, "{}Rewards:", prefix,)?;
            writeln!(
                w,
                "{}  {:<44}  {:^15}  {:<16}  {:<20}",
                prefix, "Address", "Type", "Amount", "New Balance"
            )?;
            for reward in rewards {
                let sign = if reward.lamports < 0 { "-" } else { "" };
                writeln!(
                    w,
                    "{}  {:<44}  {:^15}  {}◎{:<14.9}  ◎{:<18.9}",
                    prefix,
                    reward.pubkey,
                    if let Some(reward_type) = reward.reward_type {
                        format!("{}", reward_type)
                    } else {
                        "-".to_string()
                    },
                    sign,
                    lamports_to_sol(reward.lamports.abs() as u64),
                    lamports_to_sol(reward.post_balance)
                )?;
            }
        }
    }
    Ok(())
}

fn write_status<W: io::Write>(
    w: &mut W,
    transaction_status: &Result<(), TransactionError>,
    prefix: &str,
) -> io::Result<()> {
    writeln!(
        w,
        "{}Status: {}",
        prefix,
        match transaction_status {
            Ok(_) => "Ok".into(),
            Err(err) => err.to_string(),
        }
    )
}

fn write_fees<W: io::Write>(w: &mut W, transaction_fee: u64, prefix: &str) -> io::Result<()> {
    writeln!(w, "{}  Fee: ◎{}", prefix, lamports_to_sol(transaction_fee))
}

fn write_balances<W: io::Write>(
    w: &mut W,
    transaction_status: &UiTransactionStatusMeta,
    prefix: &str,
) -> io::Result<()> {
    assert_eq!(
        transaction_status.pre_balances.len(),
        transaction_status.post_balances.len()
    );
    for (i, (pre, post)) in transaction_status
        .pre_balances
        .iter()
        .zip(transaction_status.post_balances.iter())
        .enumerate()
    {
        if pre == post {
            writeln!(
                w,
                "{}  Account {} balance: ◎{}",
                prefix,
                i,
                lamports_to_sol(*pre)
            )?;
        } else {
            writeln!(
                w,
                "{}  Account {} balance: ◎{} -> ◎{}",
                prefix,
                i,
                lamports_to_sol(*pre),
                lamports_to_sol(*post)
            )?;
        }
    }
    Ok(())
}

fn write_log_messages<W: io::Write>(
    w: &mut W,
    log_messages: Option<&Vec<String>>,
    prefix: &str,
) -> io::Result<()> {
    if let Some(log_messages) = log_messages {
        if !log_messages.is_empty() {
            writeln!(w, "{}Log Messages:", prefix,)?;
            for log_message in log_messages {
                writeln!(w, "{}  {}", prefix, log_message)?;
            }
        }
    }
    Ok(())
}

pub fn println_transaction(
    transaction: &Transaction,
    transaction_status: Option<&UiTransactionStatusMeta>,
    prefix: &str,
    sigverify_status: Option<&[CliSignatureVerificationStatus]>,
    block_time: Option<UnixTimestamp>,
) {
    let mut w = Vec::new();
    if write_transaction(
        &mut w,
        transaction,
        transaction_status,
        prefix,
        sigverify_status,
        block_time,
        CliTimezone::Local,
    )
    .is_ok()
    {
        if let Ok(s) = String::from_utf8(w) {
            print!("{}", s);
        }
    }
}

pub fn writeln_transaction(
    f: &mut dyn fmt::Write,
    transaction: &Transaction,
    transaction_status: Option<&UiTransactionStatusMeta>,
    prefix: &str,
    sigverify_status: Option<&[CliSignatureVerificationStatus]>,
    block_time: Option<UnixTimestamp>,
) -> fmt::Result {
    let mut w = Vec::new();
    let write_result = write_transaction(
        &mut w,
        transaction,
        transaction_status,
        prefix,
        sigverify_status,
        block_time,
        CliTimezone::Local,
    );

    if write_result.is_ok() {
        if let Ok(s) = String::from_utf8(w) {
            write!(f, "{}", s)?;
        }
    }
    Ok(())
}

/// Creates a new process bar for processing that will take an unknown amount of time
pub fn new_spinner_progress_bar() -> ProgressBar {
    let progress_bar = ProgressBar::new(42);
    progress_bar
        .set_style(ProgressStyle::default_spinner().template("{spinner:.green} {wide_msg}"));
    progress_bar.enable_steady_tick(100);
    progress_bar
}

pub fn unix_timestamp_to_string(unix_timestamp: UnixTimestamp) -> String {
    match NaiveDateTime::from_timestamp_opt(unix_timestamp, 0) {
        Some(ndt) => DateTime::<Utc>::from_utc(ndt, Utc).to_rfc3339_opts(SecondsFormat::Secs, true),
        None => format!("UnixTimestamp {}", unix_timestamp),
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_sdk::{
            message::{v0::LoadedAddresses, Message as LegacyMessage, MessageHeader},
            pubkey::Pubkey,
            signature::{Keypair, Signer},
        },
        solana_transaction_status::{Reward, RewardType, TransactionStatusMeta},
        std::io::BufWriter,
    };

    fn test_keypair() -> Keypair {
        let secret = ed25519_dalek::SecretKey::from_bytes(&[0u8; 32]).unwrap();
        let public = ed25519_dalek::PublicKey::from(&secret);
        let keypair = ed25519_dalek::Keypair { secret, public };
        Keypair::from_bytes(&keypair.to_bytes()).unwrap()
    }

    #[test]
    fn test_write_transaction() {
        let keypair = test_keypair();
        let account_key = Pubkey::new_from_array([1u8; 32]);
        let transaction = Transaction::new(
            &[&keypair],
            LegacyMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                recent_blockhash: Hash::default(),
                account_keys: vec![keypair.pubkey(), account_key],
                instructions: vec![CompiledInstruction::new_from_raw_parts(1, vec![], vec![0])],
            },
            Hash::default(),
        );

        let sigverify_status = CliSignatureVerificationStatus::verify_transaction(&transaction);
        let meta = TransactionStatusMeta {
            status: Ok(()),
            fee: 5000,
            pre_balances: vec![5000, 10_000],
            post_balances: vec![0, 9_900],
            inner_instructions: None,
            log_messages: Some(vec!["Test message".to_string()]),
            pre_token_balances: None,
            post_token_balances: None,
            rewards: Some(vec![Reward {
                pubkey: account_key.to_string(),
                lamports: -100,
                post_balance: 9_900,
                reward_type: Some(RewardType::Rent),
                commission: None,
            }]),
            loaded_addresses: LoadedAddresses::default(),
        };

        let output = {
            let mut write_buffer = BufWriter::new(Vec::new());
            write_transaction(
                &mut write_buffer,
                &transaction,
                Some(&meta.into()),
                "",
                Some(&sigverify_status),
                Some(1628633791),
                CliTimezone::Utc,
            )
            .unwrap();
            let bytes = write_buffer.into_inner().unwrap();
            String::from_utf8(bytes).unwrap()
        };

        assert_eq!(
            output,
            r#"Block Time: 2021-08-10T22:16:31Z
Recent Blockhash: 11111111111111111111111111111111
Signature 0: 5pkjrE4VBa3Bu9CMKXgh1U345cT1gGo8QBVRTzHAo6gHeiPae5BTbShP15g6NgqRMNqu8Qrhph1ATmrfC1Ley3rx (pass)
Account 0: srw- 4zvwRjXUKGfvwnParsHAS3HuSVzV5cA4McphgmoCtajS (fee payer)
Account 1: -r-x 4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi
Instruction 0
  Program:   4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi (1)
  Account 0: 4zvwRjXUKGfvwnParsHAS3HuSVzV5cA4McphgmoCtajS (0)
  Data: []
Status: Ok
  Fee: ◎0.000005
  Account 0 balance: ◎0.000005 -> ◎0
  Account 1 balance: ◎0.00001 -> ◎0.0000099
Log Messages:
  Test message
Rewards:
  Address                                            Type        Amount            New Balance         \0
  4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi        rent        -◎0.000000100     ◎0.000009900       \0
"#.replace("\\0", "") // replace marker used to subvert trailing whitespace linter on CI
        );
    }

    #[test]
    fn test_format_labeled_address() {
        let pubkey = Pubkey::default().to_string();
        let mut address_labels = HashMap::new();

        assert_eq!(format_labeled_address(&pubkey, &address_labels), pubkey);

        address_labels.insert(pubkey.to_string(), "Default Address".to_string());
        assert_eq!(
            &format_labeled_address(&pubkey, &address_labels),
            "Default Address (1111..1111)"
        );

        address_labels.insert(
            pubkey.to_string(),
            "abcdefghijklmnopqrstuvwxyz1234567890".to_string(),
        );
        assert_eq!(
            &format_labeled_address(&pubkey, &address_labels),
            "abcdefghijklmnopqrstuvwxyz12345 (1111..1111)"
        );
    }
}
