"""
Account management.

Allows users to manage their account information.
"""

from pydle import Future

from kochira.service import Service, coroutine
from kochira.userdata import UserData

service = Service(__name__, __doc__)


@service.setup
def init_confirmations(ctx):
    ctx.storage.confirmations = {}


def wait_for_confirmation(storage, account, network, alt_account, alt_network):
    confirmation = Future()
    storage.confirmations[account, network,
                          alt_account, alt_network] = confirmation
    return confirmation


@service.command(r"!link (?P<account>\S+) (?P<network>\S+)")
@coroutine
def link(ctx, account, network):
    """
    Link account.

    Link your account to another network. This account will then become the
    primary account, and the other account made an alias of the account
    requesting linkage.
    """

    try:
        user_data = yield ctx.lookup_user_data()
    except UserData.DoesNotExist:
        ctx.respond(ctx._("Please log in to NickServ before linking an account."))
        return

    try:
        alt_client, *_ = [client for client in ctx.bot.clients.values()
                          if client.network == network]
    except ValueError:
        ctx.respond(ctx._("I can't find that network."))
        return

    alt_user_data = yield UserData.lookup_default(alt_client, account)

    if user_data.account == alt_user_data.account and \
       user_data.network == alt_user_data.network:
        ctx.respond(ctx._("You can't link your account to itself."))
        return

    if "_alias" in alt_user_data:
        ctx.respond(ctx._("You can't link your account to an alias."))
        return

    ctx.respond(ctx._("Okay, please message me \"!confirmlink {account} {network}\" on that network.").format(
        account=ctx.origin,
        network=ctx.client.network
    ))

    yield wait_for_confirmation(ctx.storage, ctx.origin, ctx.client.network,
                                account, network)

    data = dict(alt_user_data)
    data.update(user_data)
    user_data.update(data)
    user_data.save()

    alt_user_data.clear()
    alt_user_data["_alias"] = {
        "account": user_data.account,
        "network": user_data.network
    }
    alt_user_data.save()

    ctx.respond(ctx._("Your account has been successfully linked with {account} on {network}.").format(
        account=account,
        network=network
    ))


@service.command(r"!confirmlink (?P<account>\S+) (?P<network>\S+)")
@coroutine
def confirm_link(ctx, account, network):
    """
    Confirm link.

    Confirm a requested linkage as an alias of another account. You must
    attempt to link an account first before using this.
    """

    try:
        user_data = yield ctx.lookup_user_data()
    except UserData.DoesNotExist:
        ctx.respond(ctx._("Please log in to NickServ before confirming linkage."))
        return

    fut = ctx.storage.confirmations \
        .get((account, network, ctx.origin, ctx.client.network))

    if fut is None:
        ctx.respond(ctx._("That account hasn't requested linkage."))
        return

    ctx.respond(ctx._("Link confirmed."))

    fut.set_result(None)
