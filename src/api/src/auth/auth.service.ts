import { Injectable, UnauthorizedException } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';

// Em produção isso viria do banco de dados.
// Para o portfolio, um usuário hardcoded é suficiente
// e demonstra o padrão de autenticação corretamente.
const VALID_USERS = [
  { username: 'admin', password: 'admin123', role: 'admin' },
  { username: 'viewer', password: 'viewer123', role: 'viewer' },
];

@Injectable()
export class AuthService {
  constructor(private jwtService: JwtService) {}

  /**
   * Valida credenciais e retorna um JWT token.
   *
   * O payload do token contém: username e role.
   * Esses campos ficam disponíveis em qualquer rota protegida
   * via @Request() req — req.user.username, req.user.role
   */
  login(username: string, password: string): { access_token: string } {
    const user = VALID_USERS.find(
      (u) => u.username === username && u.password === password,
    );

    if (!user) {
      throw new UnauthorizedException('Invalid credentials');
    }

    const payload = { username: user.username, role: user.role };
    return {
      access_token: this.jwtService.sign(payload),
    };
  }
}