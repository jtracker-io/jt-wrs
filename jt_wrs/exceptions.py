__all__ = [
    'OwnerIDNotFound',
    'OwnerNameNotFound',
    'AMSNotAvailable'
]


class OwnerIDNotFound(Exception):
    def __str__(self):
        return 'Owner ID not found: %s' % (self.args[0])


class OwnerNameNotFound(Exception):
    def __str__(self):
        return 'Owner name not found: %s' % (self.args[0])


class AMSNotAvailable(Exception):
    def __str__(self):
        return 'Owner Management Service temporarily not available'
