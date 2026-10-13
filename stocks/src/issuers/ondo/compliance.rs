//! Ondo's [`Restrictions`]: whether a GM token will move for the addresses a fill passes
//! through.
//!
//! Every address past the token itself is discovered from it on each read, the way the token
//! finds them on each transfer, so a `setCompliance` or a new pause manager on Ondo's side is
//! followed without a config change:
//!
//! ```text
//! token.tokenPauseManager()                  -> isTokenPaused(token)
//! token.compliance()                         -> the GM compliance view
//!   view.compliance(), view.gmIdentifier()   -> OndoCompliance, the id the lists are kept under
//!   compliance.rwaTokenToBlocklist(id)       -> isBlocked(party)      (zero: no list)
//!   compliance.rwaTokenToSanctionsList(id)   -> isSanctioned(party)   (zero: no list)
//! ```
//!
//! These say *why* the token would refuse. Whether it does is the core guard's own question,
//! asked of the token itself by trying the transfer a fill makes (`stock::guard`), so a
//! check Ondo adds that these reads do not know still withdraws the quote, under the revert
//! it answers rather than a name.
//!
//! Each step reads everything it can at once: the pause beside the lists, the list holder
//! beside the id, each party's lists together, and the guard reads the vault beside all of it.
//! A read is four round trips deep rather than eleven, each bounded by the library's 3s RPC
//! timeout.

use std::sync::Arc;

use quote_updater::{Address, calldata::Value, eyre, prelude::BoxFuture};

use crate::{
    chain::{Calls, read_address, read_bool},
    ports::{Restrictions, Rules},
};

/// A GM token's transfer rules, discovered from the token on each read.
pub struct OndoCompliance {
    pub chain: Arc<dyn Calls>,
    pub token: Address,
}

impl Restrictions for OndoCompliance {
    fn read(&self) -> BoxFuture<'_, eyre::Result<Box<dyn Rules>>> {
        Box::pin(async move {
            let compliance = read_compliance(&*self.chain, self.token).await?;
            Ok(Box::new(OndoRules {
                chain: Arc::clone(&self.chain),
                compliance,
            }) as Box<dyn Rules>)
        })
    }
}

/// What a GM token checks on every transfer, as one read finds it: whether Ondo paused it,
/// and the lists its compliance keeps for it (zero for none).
#[derive(Clone, Copy, Debug)]
struct Compliance {
    paused: bool,
    blocklist: Address,
    sanctions: Address,
}

impl Compliance {
    /// What stops every transfer, whoever the parties.
    fn problems(&self) -> Vec<String> {
        self.paused
            .then(|| "Ondo paused the token".to_owned())
            .into_iter()
            .collect()
    }
}

/// The token's pause and its lists, discovered from the token itself.
async fn read_compliance(chain: &dyn Calls, token: Address) -> eyre::Result<Compliance> {
    let paused = async {
        let manager = read_address(chain, token, "tokenPauseManager()", &[]).await?;
        read_bool(
            chain,
            manager,
            "isTokenPaused(address)",
            &[Value::Address(token)],
        )
        .await
    };
    let lists = async {
        let view = read_address(chain, token, "compliance()", &[]).await?;
        let (compliance, id) = tokio::try_join!(
            read_address(chain, view, "compliance()", &[]),
            read_address(chain, view, "gmIdentifier()", &[]),
        )?;
        let id = [Value::Address(id)];
        tokio::try_join!(
            read_address(chain, compliance, "rwaTokenToBlocklist(address)", &id),
            read_address(chain, compliance, "rwaTokenToSanctionsList(address)", &id),
        )
    };
    let (paused, (blocklist, sanctions)) = tokio::try_join!(paused, lists)?;
    Ok(Compliance {
        paused,
        blocklist,
        sanctions,
    })
}

/// One read of a GM token's rules, with the chain to apply its lists through.
struct OndoRules {
    chain: Arc<dyn Calls>,
    compliance: Compliance,
}

impl Rules for OndoRules {
    fn problems(&self) -> Vec<String> {
        self.compliance.problems()
    }

    /// Each list `party` is on, as `vault 0x… is on Ondo's blocklist`.
    fn refusals<'a>(
        &'a self,
        name: &'a str,
        party: Address,
    ) -> BoxFuture<'a, eyre::Result<Vec<String>>> {
        Box::pin(async move {
            let arg = [Value::Address(party)];
            let on = |list: Address, sig: &'static str| {
                let arg = &arg;
                async move {
                    if list.is_zero() {
                        return Ok(false);
                    }
                    read_bool(&*self.chain, list, sig, arg).await
                }
            };
            let (blocked, sanctioned) = tokio::try_join!(
                on(self.compliance.blocklist, "isBlocked(address)"),
                on(self.compliance.sanctions, "isSanctioned(address)"),
            )?;
            let mut refused = Vec::new();
            if blocked {
                refused.push(format!("{name} {party:#x} is on Ondo's blocklist"));
            }
            if sanctioned {
                refused.push(format!("{name} {party:#x} is on the sanctions list"));
            }
            Ok(refused)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::fake::FakeChain;

    const TOKEN: Address = Address::repeat_byte(0x14);
    const MANAGER: Address = Address::repeat_byte(0x21);
    const VIEW: Address = Address::repeat_byte(0x22);
    const COMPLIANCE: Address = Address::repeat_byte(0x23);
    /// The id Ondo keeps a GM token's lists under: not the token's address.
    const ID: Address = Address::repeat_byte(0x24);
    const BLOCKLIST: Address = Address::repeat_byte(0x25);
    const SANCTIONS: Address = Address::repeat_byte(0x26);
    const VAULT: Address = Address::repeat_byte(0x01);
    const PROPAMM: Address = Address::repeat_byte(0x77);

    /// Ondo's stack around `TOKEN` as the chain holds it, answering each contract's calls
    /// only with the arguments the token itself passes, with `lists` as the compliance keeps
    /// them under `ID` (zero for none), and `blocked` and `sanctioned` among the vault and the
    /// PropAMM.
    fn ondo(
        paused: bool,
        lists: (Address, Address),
        blocked: &[Address],
        sanctioned: &[Address],
    ) -> OndoCompliance {
        let address = |a: Address| vec![Value::Address(a)];
        let mut chain = FakeChain::default()
            .answer(
                TOKEN,
                "tokenPauseManager()",
                &[],
                "address",
                &address(MANAGER),
            )
            .answer(
                MANAGER,
                "isTokenPaused(address)",
                &address(TOKEN),
                "bool",
                &[Value::Bool(paused)],
            )
            .answer(TOKEN, "compliance()", &[], "address", &address(VIEW))
            .answer(VIEW, "compliance()", &[], "address", &address(COMPLIANCE))
            .answer(VIEW, "gmIdentifier()", &[], "address", &address(ID))
            .answer(
                COMPLIANCE,
                "rwaTokenToBlocklist(address)",
                &address(ID),
                "address",
                &address(lists.0),
            )
            .answer(
                COMPLIANCE,
                "rwaTokenToSanctionsList(address)",
                &address(ID),
                "address",
                &address(lists.1),
            );
        for party in [VAULT, PROPAMM] {
            let on = |listed: &[Address]| [Value::Bool(listed.contains(&party))];
            chain = chain
                .answer(
                    BLOCKLIST,
                    "isBlocked(address)",
                    &address(party),
                    "bool",
                    &on(blocked),
                )
                .answer(
                    SANCTIONS,
                    "isSanctioned(address)",
                    &address(party),
                    "bool",
                    &on(sanctioned),
                );
        }
        OndoCompliance {
            chain: Arc::new(chain),
            token: TOKEN,
        }
    }

    /// The rules one read finds, applied to the vault and the PropAMM.
    async fn judged(ondo: OndoCompliance) -> (Vec<String>, Vec<String>, Vec<String>) {
        let rules = ondo.read().await.unwrap();
        (
            rules.problems(),
            rules.refusals("vault", VAULT).await.unwrap(),
            rules.refusals("PropAMM", PROPAMM).await.unwrap(),
        )
    }

    #[tokio::test]
    async fn the_pause_and_each_list_are_read_where_ondo_keeps_them() {
        let (problems, vault, propamm) =
            judged(ondo(true, (BLOCKLIST, SANCTIONS), &[VAULT], &[PROPAMM])).await;
        assert_eq!(problems, vec!["Ondo paused the token"]);
        assert_eq!(
            vault,
            vec![format!("vault {VAULT:#x} is on Ondo's blocklist")]
        );
        assert_eq!(
            propamm,
            vec![format!("PropAMM {PROPAMM:#x} is on the sanctions list")]
        );

        let (problems, vault, propamm) =
            judged(ondo(false, (BLOCKLIST, SANCTIONS), &[], &[])).await;
        assert!(problems.is_empty() && vault.is_empty() && propamm.is_empty());
    }

    #[tokio::test]
    async fn a_list_the_compliance_keeps_none_of_refuses_nobody() {
        // Zero for either list: no call to the zero address, which would fail the read.
        let (_, vault, propamm) =
            judged(ondo(false, (Address::zero(), SANCTIONS), &[], &[PROPAMM])).await;
        assert!(vault.is_empty());
        assert_eq!(propamm.len(), 1);
        let (_, vault, propamm) =
            judged(ondo(false, (Address::zero(), Address::zero()), &[], &[])).await;
        assert!(vault.is_empty() && propamm.is_empty());
    }

    #[test]
    fn a_pause_stops_every_transfer_and_says_ondo_did_it() {
        let lists = |paused| Compliance {
            paused,
            blocklist: Address::zero(),
            sanctions: Address::zero(),
        };
        assert_eq!(lists(false).problems(), Vec::<String>::new());
        assert_eq!(lists(true).problems(), vec!["Ondo paused the token"]);
    }
}
