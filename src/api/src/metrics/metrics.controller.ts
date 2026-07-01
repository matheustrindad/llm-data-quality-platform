import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import {
  ApiTags, ApiOperation, ApiQuery,
  ApiResponse, ApiBearerAuth,
} from '@nestjs/swagger';
import { AuthGuard } from '@nestjs/passport';
import { MetricsService } from './metrics.service';

@ApiTags('metrics')
@ApiBearerAuth()
@UseGuards(AuthGuard('jwt'))
@Controller('metrics')
export class MetricsController {
  constructor(private metricsService: MetricsService) {}

  /**
   * GET /metrics
   * Métricas de qualidade agregadas por período.
   */
  @Get()
  @ApiOperation({ summary: 'Get data quality metrics aggregated by period' })
  @ApiQuery({ name: 'period', required: false, example: 7,
              description: 'Rolling window in days' })
  @ApiQuery({ name: 'source', required: false, example: 'adzuna_api' })
  @ApiResponse({ status: 200, description: 'Returns quality metrics summary' })
  @ApiResponse({ status: 401, description: 'Unauthorized' })
  getMetrics(
    @Query('period') period?: number,
    @Query('source') source?: string,
  ) {
    return this.metricsService.getQualityMetrics({ period, source });
  }

  /**
   * GET /metrics/skills
   * Top skills mais pedidas por país.
   */
  @Get('skills')
  @ApiOperation({ summary: 'Get top demanded skills by country' })
  @ApiQuery({ name: 'country', required: false, example: 'US' })
  @ApiQuery({ name: 'limit',   required: false, example: 10 })
  @ApiResponse({ status: 200, description: 'Returns top skills ranking' })
  getSkills(
    @Query('country') country?: string,
    @Query('limit')   limit?:   number,
  ) {
    return this.metricsService.getTopSkills({ country, limit });
  }
}