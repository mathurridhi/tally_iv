class StediClient:
    def __init__(self, api_key, api_url):
        self.api_key = api_key
        self.api_url = api_url

    def send_request(self, payload):
        import requests

        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(self.api_url, json=payload, headers=headers)

            # Capture response details
            result = {
                "status_code": response.status_code,
                "success": response.status_code == 200
            }

            # Try to get JSON response
            try:
                result["response"] = response.json()
            except:
                result["response"] = response.text

            return result
        except Exception as e:
            return {
                "status_code": 0,
                "success": False,
                "error": str(e)
            }

    def handle_response(self, response):
        if response:
            # Process the response as needed
            return response
        else:
            # Handle error case
            return {"error": "Failed to process request"}