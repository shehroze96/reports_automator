import helpers as db_helpers

def run():
    aurora_credentials = None
    azure_credentials = None
    db_name = input('Welcome to Zoovu Support\'s MSFT reports generator.\nWhich database would you like to query?\nFor Orca type or then hit Enter\nFor Barracuda type BA then hit Enter\nFor us-st please type ST\n')


    if db_name.strip().lower() == 'or':
        print('You have chosen Aurora, fetching the credentials and connecting to DB...')
        aurora_credentials = db_helpers.fetch_credentials('./credentials/aurora_credentials.csv')
        conn_aurora= db_helpers.create_conn_postgre(host=aurora_credentials['host'], 
                            port=aurora_credentials['port'], 
                            user=aurora_credentials['user'], 
                            password=aurora_credentials['password'], 
                            database=aurora_credentials['database'])
        
        start_date = input('Enter the start date in the following format YYYY-MM-DD: \n')
        end_date = input('Enter the end date in the following format YYYY-MM-DD: \n')
        file_name = input('Enter the file name that contains schema and assistant details (for example "msft_tiger_accounts.csv or msft_us-st_accounts.csv"): \n')
        server_name  = 'Orca'
        month_as_string = input('Enter the month you\'re querying as a string\n')


        db_helpers.five_second_delay()
        queries_dict = db_helpers.form_queries(filepath=file_name, start=start_date, end=end_date)
        db_helpers.run_query(queries_dict, conn_aurora, month_as_string, server_name)

        print('now we close the connection')
        conn_aurora.close()
 
    elif db_name.strip().lower() == 'ba':
        print('You have chosen Barracuda, fetching the credentials and connecting to DB...')
        azure_credentials = db_helpers.fetch_credentials('./credentials/azure_credentials.csv')
        conn_azure= db_helpers.create_conn_postgre(host=azure_credentials['host'], 
                            port=azure_credentials['port'], 
                            user=azure_credentials['user'], 
                            password=azure_credentials['password'], 
                            database=azure_credentials['database'])
        db_helpers.five_second_delay()
        start_date = input('Enter the start date in the following format YYYY-MM-DD:\n')
        end_date = input('Enter the start date in the following format YYYY-MM-DD:\n')
        file_name = input('Enter the file name that contains schema and assistant details (for example "msft_barracuda_accounts.csv"):\n')
        server_name  = 'Barracuda'
        month_as_string = input('Enter the month you\'re querying as a string\n')

        queries_dict = db_helpers.form_queries(filepath=file_name, start=start_date, end=end_date)
        db_helpers.run_query(queries_dict, conn_azure, month_as_string, server_name)

        print('now we close the connection')
        conn_azure.close()

    
    elif db_name.strip().lower() == 'st':
        print('You have chosen ST, fetching the credentials and connecting to DB...')
        azure_credentials = db_helpers.fetch_credentials('./credentials/st_credentials.csv')
        conn_azure= db_helpers.create_conn_postgre(host=azure_credentials['host'], 
                            port=azure_credentials['port'], 
                            user=azure_credentials['user'], 
                            password=azure_credentials['password'], 
                            database=azure_credentials['database'])
        db_helpers.five_second_delay()
        start_date = input('Enter the start date in the following format YYYY-MM-DD:\n')
        end_date = input('Enter the start date in the following format YYYY-MM-DD:\n')
        file_name = input('Enter the file name that contains schema and assistant details (for example "msft_barracuda_accounts.csv"):\n')
        server_name  = 'ST'
        month_as_string = input('Enter the month you\'re querying as a string\n')

        queries_dict = db_helpers.form_queries(filepath=file_name, start=start_date, end=end_date)
        db_helpers.run_query(queries_dict, conn_azure, month_as_string, server_name)

        print('now we close the connection')
        conn_azure.close()

    
    else:
        print('You have entered an invalid db, please choose either Aurora or Azure')


print('Time to run the program')
run()
