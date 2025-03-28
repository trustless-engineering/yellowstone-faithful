
        // uncomment and implement instructions as needed
        /*
        let instructions = message
            .instructions()
            .iter()
            .map(|i| {
                (
                    i.program_id_index,
                    i.accounts
                        .iter()
                        .map(|&index| message.account_keys()[index as usize].to_string())
                        .collect::<Vec<String>>(),
                    bs58::encode(&i.data).into_string(),
                )
            })
            .collect();
        */

        // uncomment and implement inner_instructions as needed
        /*
        let inner_instructions = meta
            .inner_instructions
            .unwrap_or_default()
            .into_iter()
            .flat_map(|ii| {
                ii.instructions.into_iter().map(|i| {
                    (
                        i.instruction
                            .accounts
                            .iter()
                            .map(|&index| message.account_keys()[index as usize].to_string())
                            .collect::<Vec<String>>(),
                        bs58::encode(&i.instruction.data).into_string(),
                    )
                })
            })
            .collect();
        */