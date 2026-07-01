import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PassportModule } from '@nestjs/passport';
import { AuthModule } from './auth/auth.module';
import { DataModule } from './data/data.module';
import { MetricsModule } from './metrics/metrics.module';

@Module({
  imports: [
    PassportModule.register({ defaultStrategy: 'jwt' }),
    TypeOrmModule.forRoot({
      type: 'postgres',
      host:     process.env.DB_HOST     || 'localhost',
      port:     parseInt(process.env.DB_PORT || '5432'),
      username: process.env.DB_USER     || 'airflow',
      password: process.env.DB_PASSWORD || 'airflow',
      database: process.env.DB_NAME     || 'airflow',
      entities: [],
      synchronize: false,
    }),
    AuthModule,
    DataModule,
    MetricsModule,
  ],
})
export class AppModule {}