# Security Best Practices

## Environment Configuration

### DO ✅
- Use `.env.template` as a reference with PLACEHOLDER values only
- Copy `.env.template` to `.env` and fill in real credentials
- Keep `.env` in `.gitignore` (already configured)
- Use strong, unique passwords for each environment
- Rotate credentials regularly (every 90 days recommended)
- Use environment-specific files (`.env.dev`, `.env.staging`, `.env.prod`)

### DON'T ❌
- **NEVER** commit `.env` file to version control
- **NEVER** put real credentials in `.env.template`
- **NEVER** share credentials via email, chat, or screenshots
- **NEVER** use default passwords in production
- **NEVER** reuse passwords across environments

## Pre-commit Hooks

Install pre-commit hooks to automatically check for security issues:

```bash
pip install pre-commit
pre-commit install
```

This will automatically:
- Detect secrets and credentials in code
- Prevent committing `.env` files
- Check `.env.template` for real credentials
- Detect private keys
- Check for merge conflicts

## Credential Management

### Database Password
```bash
# Generate strong password
python -c "import secrets, string; chars = string.ascii_letters + string.digits + string.punctuation; print(''.join(secrets.choice(chars) for _ in range(20)))"
```

### Flask Secret Key
```bash
# Generate secret key
python -c "import secrets; print(secrets.token_hex(32))"
```

### Upstox API Token
- Obtain from Upstox Developer Portal
- Tokens expire daily - update in `.env` regularly
- Never commit tokens to git

## Production Deployment

1. **Use Environment Variables**: Set credentials via environment variables, not `.env` files
2. **Secrets Management**: Use tools like HashiCorp Vault, AWS Secrets Manager, or Azure Key Vault
3. **Access Control**: Limit who can access production credentials
4. **Audit Logs**: Enable logging for credential access
5. **Encryption**: Encrypt credentials at rest and in transit

## Incident Response

If credentials are accidentally committed:

1. **Immediately rotate** all exposed credentials
2. **Force push** to remove from git history:
   ```bash
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch .env" \
     --prune-empty --tag-name-filter cat -- --all
   ```
3. **Notify** security team
4. **Review** access logs for unauthorized use
5. **Update** `.gitignore` to prevent recurrence

## Security Checklist

- [ ] `.env` file is in `.gitignore`
- [ ] `.env.template` contains only placeholders
- [ ] Pre-commit hooks are installed
- [ ] All passwords are strong and unique
- [ ] Credentials are rotated regularly
- [ ] Production uses environment variables, not `.env` files
- [ ] Access to credentials is logged and monitored
