from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    hook = TelegramHook(
        telegram_conn_id='telegram_conn',
        token='7817168988:AAFIldOOmz2kbqzuQfe1RQ9wtvY49XYP_mE',
        chat_id='-4523720217'
    )

    dag_id = context['dag'].dag_id if hasattr(context['dag'], 'dag_id') else str(context['dag'])
    run_id = context['run_id']

    message = f"Даг {dag_id} с run_id {run_id} успешно выполнен!"

    hook.send_message(chat_id='-4523720217', text=message)

def send_telegram_failure_message(context):
    hook = TelegramHook(
        telegram_conn_id='telegram_conn',
        token='7817168988:AAFIldOOmz2kbqzuQfe1RQ9wtvY49XYP_mE',
        chat_id='-4523720217'
    )

    dag_id = context['dag'].dag_id if hasattr(context['dag'], 'dag_id') else str(context['dag'])
    run_id = context['run_id']
    task_instance_key_str = context.get('task_instance_key_str', 'Unknown Task')

    message = (f"Ошибка при выполнении дага {dag_id} с run_id {run_id}. "
               f"Задача {task_instance_key_str} завершилась неудачей.")

    hook.send_message(chat_id='-4523720217', text=message)