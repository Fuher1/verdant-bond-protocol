import { Injectable, Logger, ForbiddenException } from '@nestjs/common';
import {
  ExportableRecord,
  ExportCaller,
  ExportRole,
  ExportType,
  ExportStreamOptions,
  ExportStreamResult,
  ExportStatus,
} from '../interfaces/export.interface';

const ROLE_PERMITTED_TYPES: Record<ExportRole, ExportType[]> = {
  [ExportRole.INVESTOR]: [
    ExportType.PORTFOLIO,
    ExportType.TRANSACTIONS,
    ExportType.HOLDINGS,
    ExportType.PERFORMANCE,
  ],
  [ExportRole.ISSUER]: [
    ExportType.PORTFOLIO,
    ExportType.TRANSACTIONS,
    ExportType.HOLDINGS,
    ExportType.PERFORMANCE,
    ExportType.BONDS,
    ExportType.COUPONS,
  ],
  [ExportRole.AUDITOR]: [
    ExportType.PORTFOLIO,
    ExportType.TRANSACTIONS,
    ExportType.HOLDINGS,
    ExportType.PERFORMANCE,
    ExportType.BONDS,
    ExportType.COUPONS,
    ExportType.COMPLIANCE_AUDIT,
  ],
  [ExportRole.ADMIN]: [
    ExportType.PORTFOLIO,
    ExportType.TRANSACTIONS,
    ExportType.HOLDINGS,
    ExportType.PERFORMANCE,
    ExportType.BONDS,
    ExportType.COUPONS,
    ExportType.COMPLIANCE_AUDIT,
    ExportType.KYC_PII,
  ],
};

const FORBIDDEN_FIELDS_BY_ROLE: Record<ExportRole, string[]> = {
  [ExportRole.INVESTOR]: [
    'ssn',
    'nationalId',
    'passportNumber',
    'taxId',
    'internalRiskScore',
    'complianceNotes',
    'signingKey',
    'signingSeed',
    'privateKey',
    'nonce',
    'rawKycDocument',
  ],
  [ExportRole.ISSUER]: [
    'ssn',
    'nationalId',
    'passportNumber',
    'taxId',
    'signingSeed',
    'privateKey',
    'rawKycDocument',
  ],
  [ExportRole.AUDITOR]: [
    'signingSeed',
    'privateKey',
    'passwordHash',
  ],
  [ExportRole.ADMIN]: [
    'signingSeed',
    'privateKey',
  ],
};

@Injectable()
export class ExportStreamPipeline {
  private readonly logger = new Logger(ExportStreamPipeline.name);

  validatePermission(caller: ExportCaller, exportType: ExportType): boolean {
    const permitted = ROLE_PERMITTED_TYPES[caller.role] || [];
    return permitted.includes(exportType);
  }

  sanitizeRecordForCaller(record: ExportableRecord, caller: ExportCaller): ExportableRecord {
    // 1. Data ownership check: Investors can only export their own records
    if (caller.role === ExportRole.INVESTOR && record.ownerUserId && record.ownerUserId !== caller.userId) {
      throw new ForbiddenException('Cannot access records outside your caller ownership scope');
    }

    // 2. Forbidden field stripping based on caller role
    const forbiddenFields = FORBIDDEN_FIELDS_BY_ROLE[caller.role] || [];
    const cleanedData: Record<string, any> = {};

    for (const [key, value] of Object.entries(record.data)) {
      if (forbiddenFields.includes(key)) {
        // Strip forbidden field
        continue;
      }
      cleanedData[key] = value;
    }

    return {
      ...record,
      data: cleanedData,
    };
  }

  async streamExport<T = any>(
    exportId: string,
    exportType: ExportType,
    source: AsyncIterable<ExportableRecord[]>,
    options: ExportStreamOptions,
    chunkConsumer?: (chunk: ExportableRecord[]) => Promise<void>,
    cancellationCheck?: () => Promise<boolean>,
  ): Promise<ExportStreamResult> {
    const { caller, abortSignal } = options;

    // Validate high-level permission for export type
    if (!this.validatePermission(caller, exportType)) {
      throw new ForbiddenException(
        `Role ${caller.role} is not authorized to export type ${exportType}`,
      );
    }

    let totalRecordsProcessed = 0;
    let totalChunks = 0;

    this.logger.log(`Starting bounded streaming export ${exportId} for caller ${caller.userId} (${caller.role})`);

    try {
      for await (const chunk of source) {
        // Cancellation check via AbortSignal or external cancellation token
        if (abortSignal?.aborted || (cancellationCheck && (await cancellationCheck()))) {
          this.logger.warn(`Export ${exportId} stream was cancelled at chunk ${totalChunks}`);
          return {
            exportId,
            status: 'cancelled',
            totalRecordsProcessed,
            totalChunks,
            cancelledAt: new Date(),
          };
        }

        // Project and sanitize records for caller role
        const sanitizedChunk = chunk.map((record) => this.sanitizeRecordForCaller(record, caller));

        if (chunkConsumer) {
          await chunkConsumer(sanitizedChunk);
        }

        totalRecordsProcessed += sanitizedChunk.length;
        totalChunks++;
      }

      this.logger.log(`Export ${exportId} completed successfully. Processed ${totalRecordsProcessed} records in ${totalChunks} chunks.`);
      return {
        exportId,
        status: 'completed',
        totalRecordsProcessed,
        totalChunks,
        completedAt: new Date(),
      };
    } catch (error) {
      if (abortSignal?.aborted || (error as any)?.name === 'AbortError') {
        return {
          exportId,
          status: 'cancelled',
          totalRecordsProcessed,
          totalChunks,
          cancelledAt: new Date(),
        };
      }
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`Export ${exportId} failed: ${errorMsg}`);
      return {
        exportId,
        status: 'failed',
        totalRecordsProcessed,
        totalChunks,
        error: errorMsg,
      };
    }
  }
}
