import {
  BadRequestException,
  Body,
  Controller,
  Headers,
  Post,
  Res,
} from '@nestjs/common';
import type { Response } from 'express';
import { z } from 'zod';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrdersService } from './orders.service';

// Printable ASCII, no spaces, 1-255 chars (e.g. a UUID or client request id).
const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(255)
  .regex(/^[\x21-\x7E]+$/);

@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post()
  async create(
    @Body() dto: CreateOrderDto,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (idempotencyKey === undefined) {
      throw new BadRequestException('Idempotency-Key header is required');
    }
    const parsed = idempotencyKeySchema.safeParse(idempotencyKey);
    if (!parsed.success) {
      throw new BadRequestException(
        'Idempotency-Key must be 1-255 printable ASCII characters without spaces',
      );
    }

    const { order, created } = await this.ordersService.create(
      dto,
      parsed.data,
    );
    if (!created) res.setHeader('Idempotent-Replayed', 'true');
    // Explicit response shape: requestHash is internal and stays out of the contract.
    return {
      id: order.id,
      customerEmail: order.customerEmail,
      amount: order.amount,
      idempotencyKey: order.idempotencyKey,
      createdAt: order.createdAt,
    };
  }
}
