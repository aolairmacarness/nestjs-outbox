import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PULSAR_CLIENT } from '../pulsar/pulsar.module';
import Pulsar from 'pulsar-client';
import { Database, DRIZZLE } from '../db/db.module';
import { processedEvents } from '../db/schema';

const CONSUMER_NAME = 'order-created-consumer';

@Injectable()
export class OrderCreatedConsumer implements OnModuleInit {
  private readonly logger = new Logger(OrderCreatedConsumer.name);

  constructor(
    @Inject(PULSAR_CLIENT) private readonly client: Pulsar.Client,
    @Inject(DRIZZLE) private readonly db: Database,
  ) {}

  async onModuleInit() {
    await this.client.subscribe({
      topic: 'order.created',
      subscription: 'demo-consumer',
      subscriptionType: 'Shared',
      listener: async (msg, consumer) => {
        try {
          await this.handle(msg);
          await consumer.acknowledge(msg);
        } catch (err) {
          this.logger.error(
            'Failed to handle message',
            err instanceof Error ? err.stack : err,
          );
          consumer.negativeAcknowledge(msg);
        }
      },
    });
  }

  async handle(msg: Pulsar.Message) {
    const eventId = msg.getProperties().eventId;
    const payload = JSON.parse(msg.getData().toString());

    await this.db.transaction(async (tx) => {
      const inserted = await tx
        .insert(processedEvents)
        .values({ eventId, consumer: CONSUMER_NAME })
        .onConflictDoNothing()
        .returning();

      if (inserted.length === 0) {
        this.logger.warn(`Skipping duplicate delivery of event ${eventId}`);
        return;
      }

      this.logger.log(
        `Received order.created (event ${eventId}): ${JSON.stringify(payload)}`,
      );
    });
  }
}
