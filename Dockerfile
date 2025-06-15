FROM public.ecr.aws/lambda/python:3.10

# Set working directory
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the repo contents
COPY . .

# Set the handler to call in Lambda
CMD ["monitor.lambda_handler"]
