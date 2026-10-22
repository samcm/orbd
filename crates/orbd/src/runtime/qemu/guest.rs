//! QEMU's `GuestControl`: the shared guest channel over the virtio-serial port
//! `org.orbd.control`, whose host end is `<run>/qemu/<orb>/guest.sock`.

use super::QemuRuntime;
use crate::model::{OrbDefinition, OrbId};
use crate::runtime::vm::guest::{self, Guest, VirtioSerialConnector};
use crate::runtime::{
    BootstrapRequest, GuestControl, GuestError, OpContext, RuntimeError, RuntimeObservation,
};
use async_trait::async_trait;

impl QemuRuntime {
    pub(super) fn connector(&self, orb: OrbId) -> VirtioSerialConnector {
        VirtioSerialConnector {
            socket: self.run_paths(orb).guest_sock,
        }
    }

    fn guest_of<'a>(
        &self,
        orb: OrbId,
        connector: &'a VirtioSerialConnector,
        console_log: &'a std::path::Path,
    ) -> Result<Guest<'a>, GuestError> {
        let recipe = self
            .read_recipe(orb)
            .map_err(|e| GuestError::Unreachable(e.to_string()))?;
        Ok(Guest {
            connector,
            network: recipe.net.guest_config(&self.config.dns_servers),
            console_log,
        })
    }
}

#[async_trait]
impl GuestControl for QemuRuntime {
    async fn resync(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), GuestError> {
        let connector = self.connector(orb.id);
        let console = self.orb_paths(orb.id).console_log;
        guest::resync(self.guest_of(orb.id, &connector, &console)?, ctx).await
    }

    async fn bootstrap(
        &self,
        orb: &OrbDefinition,
        request: BootstrapRequest,
        ctx: &OpContext,
    ) -> Result<(), GuestError> {
        match self.inspect_inner(orb.id).await {
            Ok(RuntimeObservation::Running) => {}
            Ok(other) => return Err(GuestError::Unreachable(format!("orb is {other:?}"))),
            Err(RuntimeError::Cancelled | RuntimeError::DeadlineExceeded) => {
                return Err(guest::interrupted(ctx));
            }
            Err(e) => return Err(GuestError::Unreachable(e.to_string())),
        }
        let connector = self.connector(orb.id);
        let console = self.orb_paths(orb.id).console_log;
        guest::bootstrap(
            self.guest_of(orb.id, &connector, &console)?,
            orb,
            request,
            ctx,
        )
        .await
    }
}
