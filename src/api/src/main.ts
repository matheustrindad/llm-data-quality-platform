import { NestFactory } from '@nestjs/core';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // ── Swagger (OpenAPI) ──────────────────────────────────────
  // Gerado automaticamente a partir dos decorators nos controllers.
  // Disponível em http://localhost:3000/docs
  const config = new DocumentBuilder()
    .setTitle('AI Data Quality Platform API')
    .setDescription(
      'REST API for the AI Data Quality & Enrichment Platform. ' +
      'Powered by PySpark + Airflow + LLM-as-a-Judge evaluation engine.',
    )
    .setVersion('1.0')
    .addBearerAuth()           // Habilita o botão "Authorize" no Swagger UI
    .addTag('auth',    'Authentication endpoints')
    .addTag('data',    'Job market data endpoints')
    .addTag('metrics', 'Data quality metrics endpoints')
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('docs', app, document);

  await app.listen(3000);
  console.log('API running at http://localhost:3000');
  console.log('Swagger UI at http://localhost:3000/docs');
}

bootstrap();