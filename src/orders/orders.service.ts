import { ConflictException, Inject, Injectable, Logger } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { eq } from 'drizzle-orm';
import { Database, DRIZZLE } from '../db/db.module';
import { CreateOrderDto } from './dto/create-order.dto';
import { Order, orders, outbox } from '../db/schema';

export interface CreateOrderResult {
  order: Order;
  /** false when an earlier request with the same idempotency key is replayed. */
  created: boolean;
}

@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(@Inject(DRIZZLE) private readonly db: Database) {}

  async create(
    dto: CreateOrderDto,
    idempotencyKey: string,
  ): Promise<CreateOrderResult> {
    const amount = dto.amount.toFixed(2);
    const requestHash = this.fingerprint(dto.customerEmail, amount);

    return this.db.transaction(async (tx) => {
      // The UNIQUE(idempotency_key) constraint is the concurrency guard. If a
      // concurrent transaction holds an uncommitted row with the same key,
      // this statement blocks until that transaction finishes: on commit it
      // inserts nothing, on rollback it proceeds and inserts.
      const [inserted] = await tx
        .insert(orders)
        .values({
          customerEmail: dto.customerEmail,
          amount,
          idempotencyKey,
          requestHash,
        })
        .onConflictDoNothing({ target: orders.idempotencyKey })
        .returning();

      if (!inserted) {
        // Key already used by a committed order (READ COMMITTED: the
        // statement above guarantees that row is now visible to us).
        const [existing] = await tx
          .select()
          .from(orders)
          .where(eq(orders.idempotencyKey, idempotencyKey));

        if (existing.requestHash !== requestHash) {
          this.logger.warn(
            `Idempotency-Key reused with a different payload (key=${idempotencyKey}, order=${existing.id})`,
          );
          throw new ConflictException(
            'Idempotency-Key was already used with a different request payload',
          );
        }

        this.logger.log(
          `Idempotent replay (key=${idempotencyKey}, order=${existing.id})`,
        );
        return { order: existing, created: false };
      }

      // Same transaction as the order: either both rows exist or neither.
      await tx.insert(outbox).values({
        eventType: 'order.created',
        payload: {
          orderId: inserted.id,
          customerEmail: inserted.customerEmail,
          amount: inserted.amount,
        },
      });

      this.logger.log(
        `Order created (key=${idempotencyKey}, order=${inserted.id})`,
      );
      return { order: inserted, created: true };
    });
  }

  /** Stable hash of the fields that define "the same request". */
  private fingerprint(customerEmail: string, amount: string): string {
    return createHash('sha256')
      .update(JSON.stringify({ customerEmail, amount }))
      .digest('hex');
  }
}
