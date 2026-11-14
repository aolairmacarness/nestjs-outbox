import { BadRequestException } from '@nestjs/common';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';
import type { Order } from '../db/schema';

const order: Order = {
  id: '7c1e0000-0000-4000-8000-000000000000',
  customerEmail: 'a@b.com',
  amount: '49.50',
  idempotencyKey: 'key-1',
  requestHash: 'internal-hash',
  createdAt: new Date('2026-01-01T00:00:00Z'),
};
const dto = { customerEmail: 'a@b.com', amount: 49.5 };

describe('OrdersController (idempotency header handling)', () => {
  let service: { create: jest.Mock };
  let controller: OrdersController;
  let res: { setHeader: jest.Mock };

  beforeEach(() => {
    service = { create: jest.fn().mockResolvedValue({ order, created: true }) };
    res = { setHeader: jest.fn() };
    controller = new OrdersController(service as unknown as OrdersService);
  });

  const call = (key: string | undefined) =>
    controller.create(dto, key, res as never);

  it('rejects a missing Idempotency-Key with 400 and never calls the service', async () => {
    await expect(call(undefined)).rejects.toBeInstanceOf(BadRequestException);
    expect(service.create).not.toHaveBeenCalled();
  });

  it.each(['', 'has space', 'x'.repeat(256), 'tab\tkey', 'ünïcode'])(
    'rejects invalid key %j with 400',
    async (key) => {
      await expect(call(key)).rejects.toBeInstanceOf(BadRequestException);
      expect(service.create).not.toHaveBeenCalled();
    },
  );

  it('passes a valid key through and hides the internal requestHash', async () => {
    const body = await call('key-1');
    expect(service.create).toHaveBeenCalledWith(dto, 'key-1');
    expect(body).not.toHaveProperty('requestHash');
    expect(body).toMatchObject({ id: order.id, amount: '49.50' });
    expect(res.setHeader).not.toHaveBeenCalled();
  });

  it('flags replays with Idempotent-Replayed', async () => {
    service.create.mockResolvedValue({ order, created: false });
    await call('key-1');
    expect(res.setHeader).toHaveBeenCalledWith('Idempotent-Replayed', 'true');
  });
});
