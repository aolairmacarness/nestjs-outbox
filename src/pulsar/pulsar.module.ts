import { Global, Module } from '@nestjs/common';
import Pulsar from 'pulsar-client';
import { ConfigService } from '@nestjs/config';

export const PULSAR_CLIENT = Symbol('PULSAR_CLIENT');

@Global()
@Module({
  providers: [
    {
      provide: PULSAR_CLIENT,
      useFactory: (config: ConfigService) =>
        new Pulsar.Client({
          serviceUrl: config.getOrThrow<string>('PULSAR_URL'),
        }),
      inject: [ConfigService],
    },
  ],
  exports: [PULSAR_CLIENT],
})
export class PulsarModule {}
