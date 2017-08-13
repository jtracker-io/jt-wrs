__all__ = [
    'AccountIDNotFound',
    'AccountNameNotFound',
    'AMSNotAvailable'
]


class AccountIDNotFound(Exception):
    def __str__(self):
        return 'Account ID not found: %s' % (self.args[0])


class AccountNameNotFound(Exception):
    def __str__(self):
        return 'Account name not found: %s' % (self.args[0])


class AMSNotAvailable(Exception):
    def __str__(self):
        return 'Account Management Service temporarily not available'
