import { Injectable } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';

@Injectable()
export class MetricsService {
  constructor(@InjectDataSource() private dataSource: DataSource) {}

  /**
   * GET /metrics — métricas de qualidade agregadas.
   *
   * Retorna um resumo das 3 camadas de avaliação:
   * - Score médio por layer (heurística, similaridade, LLM)
   * - Distribuição de scores (high/medium/low)
   * - Taxa de registros abaixo do threshold
   * - Top motivos de rejeição
   */
  async getQualityMetrics(filters: { period?: number; source?: string }) {
    const { period = 7, source } = filters;

    const params: (string | number)[] = [period];
    let sourceFilter = '';
    if (source) {
      params.push(source);
      sourceFilter = `AND source = $${params.length}`;
    }

    // Aggregate scores by layer
    const scores = await this.dataSource.query(`
      SELECT
        ROUND(AVG(heuristic_score)::numeric,  2) AS avg_heuristic,
        ROUND(AVG(similarity_score)::numeric, 2) AS avg_similarity,
        ROUND(AVG(llm_score)::numeric,        2) AS avg_llm,
        ROUND(AVG(composite_score)::numeric,  2) AS avg_composite,
        COUNT(*)                                  AS total_evaluated,
        SUM(CASE WHEN composite_score < 60 THEN 1 ELSE 0 END) AS below_threshold
      FROM evaluation_results
      WHERE batch_date >= CURRENT_DATE - ($1 || ' days')::interval
      ${sourceFilter}
    `, params);

    // Score distribution
    const distribution = await this.dataSource.query(`
      SELECT
        SUM(CASE WHEN composite_score >= 80 THEN 1 ELSE 0 END) AS high,
        SUM(CASE WHEN composite_score >= 60
                  AND composite_score <  80 THEN 1 ELSE 0 END) AS medium,
        SUM(CASE WHEN composite_score < 60  THEN 1 ELSE 0 END) AS low
      FROM evaluation_results
      WHERE batch_date >= CURRENT_DATE - ($1 || ' days')::interval
      ${sourceFilter}
    `, params);

    // Feedback history
    const feedback = await this.dataSource.query(`
      SELECT
        feedback_run_date,
        total_evaluated,
        total_reprocessed,
        avg_score_before,
        top_reason
      FROM feedback_history
      ORDER BY feedback_run_date DESC
      LIMIT 5
    `);

    return {
      period_days:  period,
      scores:       scores[0],
      distribution: distribution[0],
      feedback_history: feedback,
      generated_at: new Date().toISOString(),
    };
  }

  /**
   * GET /metrics/skills — top skills por país.
   */
  async getTopSkills(filters: { country?: string; limit?: number }) {
    const { country, limit = 10 } = filters;
    const params: (string | number)[] = [limit];
    let countryFilter = '';
    if (country) {
      params.push(country.toUpperCase());
      countryFilter = `AND country = $${params.length}`;
    }

    return this.dataSource.query(`
      SELECT skill, country, SUM(mention_count) AS total_mentions
      FROM top_skills
      WHERE 1=1 ${countryFilter}
      GROUP BY skill, country
      ORDER BY total_mentions DESC
      LIMIT $1
    `, params);
  }
}