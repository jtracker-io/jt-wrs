__all__ = [
    'AccountNameNotFound'
]


class AccountNameNotFound(Exception):
    def __str__(self):
        return 'Account name not found: %s' % (self.args[0])