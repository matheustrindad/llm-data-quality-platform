import { Injectable } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';

/**
 * JwtStrategy — valida o token JWT em cada requisição protegida.
 *
 * Como funciona:
 * 1. O cliente envia: Authorization: Bearer <token>
 * 2. ExtractJwt.fromAuthHeaderAsBearerToken() extrai o token
 * 3. O Passport verifica a assinatura com o mesmo secret
 * 4. validate() retorna o payload — disponível em req.user
 *
 * Se o token for inválido ou expirado, retorna 401 automaticamente.
 */
@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor() {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: process.env.JWT_SECRET || 'change-me-in-production',
    });
  }

  async validate(payload: { username: string; role: string }) {
    return { username: payload.username, role: payload.role };
  }
}