/**
 * Integration tests: real PostgreSQL, real transactions, HTTP via supertest.
 * Requires DATABASE_URL pointing at a migrated database (see docker-compose.yml);
 * the suite is skipped when it is not set.
 */
import { INestApplication } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_PIPE } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { randomUUID } from 'node:crypto';
import { eq, inArray, like, sql } from 'drizzle-orm';
import { ZodValidationPipe } from 'nestjs-zod';
import type { Server } from 'node:http';
import request from 'supertest';
import { Database, DbModule, DRIZZLE } from '../db/db.module';
import { orders, outbox } from '../db/schema';
import { OrdersModule } from './orders.module';
import { OrdersService } from './orders.service';

const describeDb = process.env.DATABASE_URL ? describe : describe.skip;
const RUN = `it-${randomUUID()}`; // namespaces every key this run creates

describeDb('POST /orders idempotency (PostgreSQL)', () => {
  let app: INestApplication;
  let db: Database;

  const idOf = (r: { body: unknown }) => (r.body as { id: string }).id;
  const key = (name: string) => `${RUN}-${name}`;
  const post = (k: string | undefined, body: object) => {
    const req = request(app.getHttpServer() as Server).post('/orders');
    if (k !== undefined) req.set('Idempotency-Key', k);
    return req.send(body);
  };
  const ordersForKey = (k: string) =>
    db.select().from(orders).where(eq(orders.idempotencyKey, k));
  const outboxForOrder = (orderId: string) =>
    db
      .select()
      .from(outbox)
      .where(sql`${outbox.payload}->>'orderId' = ${orderId}`);

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        DbModule,
        OrdersModule,
      ],
      providers: [{ provide: APP_PIPE, useClass: ZodValidationPipe }],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
    db = app.get<Database>(DRIZZLE);
  });

  afterAll(async () => {
    const rows = await db
      .select({ id: orders.id })
      .from(orders)
      .where(like(orders.idempotencyKey, `${RUN}-%`));
    const ids = rows.map((r) => r.id);
    if (ids.length) {
      await db
        .delete(outbox)
        .where(inArray(sql`${outbox.payload}->>'orderId'`, ids));
      await db.delete(orders).where(inArray(orders.id, ids));
    }
    await app.close();
    await db.$client.end();
  });

  const body = { customerEmail: 'buyer@example.com', amount: 49.5 };

  it('new key: creates the order (201) and exactly one pending outbox event', async () => {
    const k = key('new');
    const res = await post(k, body).expect(201);

    expect(res.body).toMatchObject({
      customerEmail: body.customerEmail,
      amount: '49.50',
      idempotencyKey: k,
    });
    expect(res.body).not.toHaveProperty('requestHash');

    expect(await ordersForKey(k)).toHaveLength(1);
    const events = await outboxForOrder(idOf(res));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      eventType: 'order.created',
      status: 'pending',
    });
  });

  it('same key + same payload: returns the original response, no new order or event', async () => {
    const k = key('replay');
    const first = await post(k, body).expect(201);
    const second = await post(k, body).expect(201);

    expect(second.body).toEqual(first.body);
    expect(second.headers['idempotent-replayed']).toBe('true');
    expect(first.headers['idempotent-replayed']).toBeUndefined();
    expect(await ordersForKey(k)).toHaveLength(1);
    expect(await outboxForOrder(idOf(first))).toHaveLength(1);
  });

  it('same key + different payload: 409 and nothing changes', async () => {
    const k = key('conflict');
    const first = await post(k, body).expect(201);
    await post(k, { ...body, amount: 99 }).expect(409);
    await post(k, { ...body, customerEmail: 'other@example.com' }).expect(409);

    const rows = await ordersForKey(k);
    expect(rows).toHaveLength(1);
    expect(rows[0].amount).toBe('49.50');
    expect(await outboxForOrder(idOf(first))).toHaveLength(1);
  });

  it('missing / invalid key: 400 and nothing is written', async () => {
    const before = await db
      .select({ n: sql<number>`count(*)::int` })
      .from(orders);
    await post(undefined, body).expect(400);
    await post('has space', body).expect(400);
    const after = await db
      .select({ n: sql<number>`count(*)::int` })
      .from(orders);
    expect(after[0].n).toBe(before[0].n);
  });

  it('invalid body with a valid key: 400 and the key is not consumed', async () => {
    const k = key('bad-body');
    await post(k, { customerEmail: 'not-an-email', amount: 5 }).expect(400);
    expect(await ordersForKey(k)).toHaveLength(0);
  });

  it('concurrent requests with the same key: one order, one event, identical responses', async () => {
    const k = key('concurrent');
    const responses = await Promise.all(
      Array.from({ length: 8 }, () => post(k, body)),
    );

    expect(responses.map((r) => r.status)).toEqual(Array(8).fill(201));
    const ids = new Set(responses.map((r) => idOf(r)));
    expect(ids.size).toBe(1);
    for (const r of responses) expect(r.body).toEqual(responses[0].body);

    expect(await ordersForKey(k)).toHaveLength(1);
    expect(await outboxForOrder([...ids][0])).toHaveLength(1);
    // exactly one request was the creator; the rest were replays
    const replays = responses.filter(
      (r) => r.headers['idempotent-replayed'] === 'true',
    );
    expect(replays).toHaveLength(7);
  });

  it('concurrent requests, same key, different payloads: one winner, the rest 409', async () => {
    const k = key('concurrent-conflict');
    const responses = await Promise.all(
      [10, 20, 30, 40].map((amount) => post(k, { ...body, amount })),
    );
    const statuses = responses.map((r) => r.status).sort();
    expect(statuses).toEqual([201, 409, 409, 409]);
    expect(await ordersForKey(k)).toHaveLength(1);
  });

  it('rollback: if the outbox insert fails, no order and no idempotency record remain', async () => {
    const k = key('rollback');
    const failingDb = {
      transaction: (fn: (tx: unknown) => Promise<unknown>) =>
        db.transaction((tx) =>
          fn(
            new Proxy(tx, {
              get: (target, prop, recv) =>
                prop === 'insert'
                  ? (table: unknown) => {
                      if (table === outbox) throw new Error('outbox boom');
                      return target.insert(table as typeof orders);
                    }
                  : (Reflect.get(target, prop, recv) as unknown),
            }),
          ),
        ),
    };
    const brokenService = new OrdersService(failingDb as unknown as Database);

    await expect(
      brokenService.create({ customerEmail: 'x@example.com', amount: 5 }, k),
    ).rejects.toThrow('outbox boom');
    expect(await ordersForKey(k)).toHaveLength(0);
  });

  it('retry after an interrupted request: the key was not burned, retry creates exactly one order', async () => {
    const k = key('interrupted');
    // First attempt dies mid-transaction (after the order insert, before commit).
    await expect(
      db.transaction(async (tx) => {
        await tx.insert(orders).values({
          customerEmail: body.customerEmail,
          amount: '49.50',
          idempotencyKey: k,
          requestHash: 'whatever',
        });
        throw new Error('connection lost');
      }),
    ).rejects.toThrow('connection lost');
    expect(await ordersForKey(k)).toHaveLength(0);

    const res = await post(k, body).expect(201);
    expect(await ordersForKey(k)).toHaveLength(1);
    expect(await outboxForOrder(idOf(res))).toHaveLength(1);
  });

  it('schema: idempotency_key is enforced unique by the database itself', async () => {
    const k = key('db-unique');
    const row = {
      customerEmail: 'u@example.com',
      amount: '1.00',
      idempotencyKey: k,
    };
    await db.insert(orders).values(row);
    await expect(db.insert(orders).values(row)).rejects.toThrow();
    expect(await ordersForKey(k)).toHaveLength(1);
  });
});
