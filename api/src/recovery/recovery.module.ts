import { Module } from '@nestjs/common';
import { RecoveryService } from './recovery.service';
import { RecoveryController } from './recovery.controller';
import { RollbackVerificationService } from './rollback-verification.service';
import { IncidentDrillService } from './incident-drill.service';
import { BondStateMachine, CouponStateMachine, SettlementStateMachine } from '../bonds/bond.state-machine';
import { NotificationsModule } from '../notifications/notifications.module';

@Module({
  imports: [NotificationsModule],
  controllers: [RecoveryController],
  providers: [
    RecoveryService,
    RollbackVerificationService,
    IncidentDrillService,
    BondStateMachine,
    CouponStateMachine,
    SettlementStateMachine,
  ],
  exports: [
    RecoveryService,
    RollbackVerificationService,
    IncidentDrillService,
  ],
})
export class RecoveryModule {}
