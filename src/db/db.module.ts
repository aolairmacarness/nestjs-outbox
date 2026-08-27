import { drizzle } from 'drizzle-orm/postgres-js';
import * as schema from './schema';
import postgres from 'postgres';
import { Global, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export const DRIZZLE = Symbol('DRIZZLE');

export type Database = ReturnType<typeof createDb>;

function createDb(url: string) {
  const client = postgres(url);
  return drizzle(client, { schema });
}

@Global()
@Module({
  providers: [
    {
      provide: DRIZZLE,
      useFactory: (config: ConfigService) =>
        createDb(config.getOrThrow<string>('DATABASE_URL')),
      inject: [ConfigService],
    },
  ],
  exports: [DRIZZLE],
})
export class DbModule {}
