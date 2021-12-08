#!/usr/bin/env python3

from re import fullmatch
import click
import queries
import SQLDriver
from os import getenv
from AlphaVantageDriver import AlphaVantageDriver, DatedDict
from datetime import date, datetime, timedelta

def gendriver(driver_type = None, user = None, password = None, host = None, port = None, dbname = None):
    if driver_type == "sqlite":
        return SQLDriver.SQLite(file = dbname)
    elif driver_type == "mysql":
        return SQLDriver.MySQL(host = host, database = dbname, user = user, pw = password, port = port)
    elif driver_type == "postgresql":
        return SQLDriver.PgSQL(host = host, database = dbname, user = user, pw = password, port = port)
    else:
        raise ValueError(f"Invalid driver type! Expected one of \"sqlite\", \"mysql\", \"postgresql\", got \"{driver_type}\"")

verbose_log = False

def log(txt):
    if verbose_log:
        print(txt)

@click.group()
@click.version_option(version = "0.1.0")
@click.option("--driver", "-d", required = True, type = click.Choice(("sqlite", "mysql", "postgresql"), case_sensitive = False), help = "The database driver to use.")
@click.option("--user", "-u", required = False, metavar = "<user>")
@click.option("--password", "-p", required = False, metavar = "<password>")
@click.option("--host", "-H", required = False, metavar = "<hostname>")
@click.option("--port", "-P", required = False, metavar = "<port>")
@click.option("--dbname", "--name", "-D", required = False, metavar = "<database name>")
@click.option("--avkey", "--av", "-A", required = False, metavar = "<api key>")
@click.option("--verbose", "-V", is_flag = True, required = False, default = False)
@click.pass_context
def cli(ctx, driver, user, password, host, port, dbname, avkey, verbose):
    """
    Online quotes driver for GnuCash.
    """
    global verbose_log
    verbose_log = True if verbose else False
    ctx.ensure_object(dict)

    ctx.obj["file_driver"] = gendriver(
        driver_type = driver,
        user = user if user is not None else getenv("GC_QUOTES_DB_USER"),
        password = password if password is not None else getenv("GC_QUOTES_DB_PW"),
        host = host if host is not None else getenv("GC_QUOTES_DB_HOST"),
        port = port if port is not None else getenv("GC_QUOTES_DB_PORT"),
        dbname = dbname if dbname is not None else getenv("GC_QUOTES_DB_NAME")
    )

    ctx.obj["av_driver"] = AlphaVantageDriver(apikey = avkey if avkey is not None else getenv("GC_QUOTES_AV_KEY"))

@cli.command(help="Adds prices of the tracked tickers to the database")
@click.pass_context
@click.option("--from-date", required = True, type = click.DateTime())
@click.option("--to-date", required = False, type = click.DateTime(), default = date.today().isoformat)
@click.option("--until-yesterday", required = False, is_flag = True, default = False)
def fill(ctx, from_date, to_date, until_yesterday):
    from_date = from_date.date()
    to_date = to_date.date()

    if until_yesterday:
        to_date = date.today() - timedelta(days = 1)
    if from_date > to_date:
        raise ValueError("End date cannot be before the start date!")

    quotes_global = []
    extended_data = (to_date - from_date).days > 90
    av = ctx.obj["av_driver"]
    with ctx.obj["file_driver"] as driver:
        for row in driver.execute(queries.fetch_commodities()):
            log(f"Fetching quotes for {row[1]}")
            qts = None
            if row[0] == "CURRENCY":
                qts = av.fx(symbol = row[1], to = row[2], full_history = extended_data)
            elif row[0] in ("CRYPTO", "CRYPTOCURRENCY"):
                qts = av.crypto(symbol = row[1], to = row[2])
            else:
                qts = av.mkt(symbol = row[1], full_history = extended_data)

            for quote in qts.iter(to_date, from_date):
                if quote[1] is None:
                    continue
                price = quote[1]
                if row[2] == "GBP" and row[0] not in ("CURRENCY", "CRYPTO", "CRYPTOCURRENCY"):
                    # Prices are displayed in GBX (Pence Sterling) rather than GBP (Pound Sterling)
                    # It's like displaying prices in cents or something like that. We need to handle
                    # this edge case manually.
                    price = price / 100

                quotes_global.append((row[3], row[4], quote[0], price))
        
        log("Inserting prices...")
        query, query_params = queries.insert_prices(quotes_global)
        driver.execute(query, query_params)

if __name__ == "__main__":
    cli()
