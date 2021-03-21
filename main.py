import cmd
import sqlite3
import datetime

from prettytable import PrettyTable


class Bank(cmd.Cmd):
    """ Небольшой сервис, имитирующий работу банка со счетами клиентов. """

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = '> '
        self.intro = 'Service started!'
        self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.cursor()

    def _check_argument(self, argument):
        return argument.startswith('--')

    def _get_cmd_args(self, args):
        if self._check_argument(args):
            clear_args = str(args).replace(' -', '-').replace("'", '').replace('"', '')
            if 'amount' in clear_args:
                _, _client, _amount, _description = clear_args.split('--')
                client = _client.replace('client=', '')
                amount = _amount.replace('amount=', '')
                description = _description.replace('description=', '')
                return client, amount, description
            elif 'since' in clear_args:
                _, _client, _since, _till = clear_args.split('--')
                client = _client.replace('client=', '')
                since = _since.replace('since=', '')
                till = _till.replace('till=', '')
                return client, since, till

    def create_db(self):
        self.cursor.executescript(
            """
            PRAGMA foreign_keys=on;
            CREATE TABLE IF NOT EXISTS client_operations(
                client_name TEXT,
                description TEXT,
                withdrawal REAL,
                deposit REAL,
                balance REAL,
                date_add TIMESTAMP,
                FOREIGN KEY (client_name) REFERENCES clients(name)
            );
            CREATE TABLE clients(
                name TEXT PRIMARY KEY,
                balance REAL
            );
            """
        )
        self.connection.commit()

    def _insert_into_db(self, args, operation_status=True):
        data = self._get_cmd_args(args)
        if data:
            deposit_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            client, amount, description = data
            balance_list = []
            if operation_status:
                balance = 0 + float(amount)
            else:
                balance = 0 - float(amount)

            try:
                client_amount = (client, balance)
                self.cursor.execute('INSERT INTO clients VALUES(?, ?);', client_amount)
                balance_list.append(balance)

            except sqlite3.IntegrityError:
                self.cursor.execute(f"SELECT balance FROM clients WHERE name = '{client}';")
                balance_info = self.cursor.fetchone()[0]

                if operation_status:
                    result_balance = float(balance_info) + float(amount)
                else:
                    result_balance = float(balance_info) - float(amount)
                self.cursor.execute(f"UPDATE clients SET balance = {result_balance} WHERE name = '{client}';")
                balance_list.append(result_balance)
            finally:
                self.connection.commit()

            if operation_status:
                insert_into_db = (client, description, float(amount), None, balance_list[-1], deposit_date)
            else:
                insert_into_db = (client, description, None, float(amount), balance_list[-1], deposit_date)

            self.cursor.execute('INSERT INTO client_operations VALUES(?, ?, ?, ?, ?, ?);', insert_into_db)
            self.connection.commit()
            return True

    def do_deposit(self, args):
        if self._insert_into_db(args=args):
            print('Deposit operation was successful!')
        else:
            print('Error! The required parameters are missing.\nType "help" for help.')

    def do_withdraw(self, args):
        if self._insert_into_db(args=args, operation_status=False):
            print('Withdrawal operation was successful!')
        else:
            print('Error! The required parameters are missing.\nType "help" for help.')

    def do_show_bank_statement(self, args):
        client, since, till = self._get_cmd_args(args)
        print(since, till)
        self.cursor.execute("SELECT * "
                            "FROM client_operations "
                            f"WHERE ('{since}' <= date_add and date_add <= '{till}') and  (client_name='{client}');")
        result = self.cursor.fetchall()

        self.cursor.execute("SELECT * "
                            "FROM client_operations "
                            f"WHERE ('{since}' > date_add and date_add) and  (client_name='{client}');")
        result_previous_balance = self.cursor.fetchall()

        if result_previous_balance:
            previous_balance = '$' + str(result_previous_balance[-1][4])
        else:
            previous_balance = '$0.00'

        if result:
            th = ['Date', 'Description', 'Withdrawals', 'Deposits', 'Balance']
            table = PrettyTable(th)
            table.add_row(('', 'Previous balance', '', '', previous_balance))
            table.add_row(('-------------------', '----------------', '-----------', '--------', '-------'))
            total_deposits = 0
            total_withdrawals = 0
            total_balance = 0
            for record in result:
                name, description, deposits, withdrawals, balance, date = record
                total_balance = balance
                balance = '$' + str(balance) + '0'
                if withdrawals is None:
                    withdrawals = ''
                else:
                    total_withdrawals += withdrawals
                    withdrawals = '$' + str(withdrawals) + '0'
                if deposits is None:
                    deposits = ''
                else:
                    total_deposits += deposits
                    deposits = '$' + str(deposits) + '0'

                table.add_row((date, description, withdrawals, deposits, balance))

            if total_withdrawals > 0:
                total_withdrawals = '$' + str(total_withdrawals) + '0'
            else:
                total_withdrawals = ''
            if total_deposits > 0:
                total_deposits = '$' + str(total_deposits) + '0'
            else:
                total_deposits = ''
            total_balance = '$' + str(total_balance) + '0'

            table.add_row(('-------------------', '----------------', '-----------', '--------', '-------'))
            table.add_row(('', 'Totals', total_withdrawals, total_deposits, total_balance))
            print(table)
        else:
            print('Error! Data not found')

    def do_help(self, args):
        info = """ Поддерживаемые операции:
1) deposit - операция пополнения счета на сумму, аргументы: client, amount, description
    Пример - deposit --client="John Jones" --amount=100 --description="ATM Deposit"
2) withdraw - операция снятия со счета, аргументы: client, amount, description
    Пример - withdraw --client="John Jones" --amount=50 --description="ATM Withdrawal" 
3) show_bank_statement - вывод на экран выписки со счета за период, аргументы: client, since, till
    Пример show_bank_statement --client="John Jones" --since="2021-01-01 00:00:00" --till="2021-04-01 00:00:00"
4) exit - для выхода из программы """
        print(info)

    def do_exit(self, args):
        print('End of session...')
        exit()

    def default(self, line):
        print('Error! Non-existing command.\nType "help" for help.')


if __name__ == "__main__":
    bank = Bank()
    bank.create_db()
    try:
        bank.cmdloop()
    except KeyboardInterrupt:
        print('End of session...')
