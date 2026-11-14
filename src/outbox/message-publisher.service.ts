import { Inject, Injectable, Logger } from '@nestjs/common';
import { Interval } from '@nestjs/schedule';
import { Database, DRIZZLE } from '../db/db.module';
import { outbox } from '../db/schema';
import { and, asc, eq, lte, sql } from 'drizzle-orm';
import { MessageBrokerService } from './message-broker.service';

const POLL_INTERVAL_MS = Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 5000);
const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE ?? 10);
const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
const BACKOFF_BASE_MS = Number(process.env.OUTBOX_BACKOFF_BASE_MS ?? 2000);
const BACKOFF_MAX_MS = Number(process.env.OUTBOX_BACKOFF_MAX_MS ?? 300_000);

@Injectable()
export class OutboxPublisherService {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private draining = false;

  constructor(
    @Inject(DRIZZLE) private readonly db: Database,
    private readonly broker: MessageBrokerService,
  ) {}

  @Interval(POLL_INTERVAL_MS)
  async poll() {
    if (this.draining) return;
    this.draining = true;
    try {
      await this.drain();
    } catch (err) {
      this.logger.error(
        'Outbox drain failed',
        err instanceof Error ? err.stack : err,
      );
    } finally {
      this.draining = false;
    }
  }

  private async drain() {
    await this.db.transaction(async (tx) => {
      const batch = await tx
        .select()
        .from(outbox)
        .where(
          and(
            eq(outbox.status, 'pending'),
            lte(outbox.availableAt, sql`now()`),
          ),
        )
        .orderBy(asc(outbox.createdAt))
        .limit(BATCH_SIZE)
        .for('update', { skipLocked: true });

      if (batch.length === 0) return;

      for (const event of batch) {
        try {
          await this.broker.publish(event.eventType, event.payload, event.id);
          // if (!event.lastError) {
          //   throw new Error('Dummy error');
          // }
          await tx
            .update(outbox)
            .set({ status: 'published', publishedAt: new Date() })
            .where(eq(outbox.id, event.id));
        } catch (err) {
          const attempts = event.retryCount + 1;
          const dead = attempts >= MAX_ATTEMPTS;
          const delayMs = Math.min(
            BACKOFF_BASE_MS * 2 ** (attempts - 1),
            BACKOFF_MAX_MS,
          );
          await tx
            .update(outbox)
            .set({
              retryCount: attempts,
              lastError: err instanceof Error ? err.message : String(err),
              ...(dead
                ? { status: 'failed' as const }
                : {
                    availableAt: sql`now() + (${delayMs} * interval '1 millisecond')`,
                  }),
            })
            .where(eq(outbox.id, event.id));
          this.logger.warn(
            dead
              ? `Publish failed for ${event.id}; giving up after ${attempts} attempts (status=failed)`
              : `Publish failed for ${event.id} (attempt ${attempts}/${MAX_ATTEMPTS}); retry in ${delayMs}ms`,
          );
        }
      }
    });
  }
}
