from airflow.providers.http.hooks.http import HttpHook


def azure_automation_webhook(hook_id: str) -> int:
    """
    Triggers a HTTP hook with the given hook_id to stop a virtual machine.

    To run the webhook accordingly the host and the password in the connection needs to be as follows:
    host: base_url | example = https://webhook.dewc.azure-automation.net
    password: endpoint?token=<token> | example = webhooks?token=<token>

    :param hook_id: A string representing the ID of the HttpHook to be triggered.
    :type hook_id: str
    :return: The status code of the response.
    :rtype: int
    """
    hook = HttpHook(http_conn_id=hook_id)
    connection = hook.get_connection(hook_id)
    password = connection.password
    return hook.run(endpoint=password).status_code
