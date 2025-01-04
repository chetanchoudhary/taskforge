import json
from typing import Any, Dict

import yaml
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi


def custom_openapi(app: FastAPI) -> Dict[str, Any]:
    """Generate custom OpenAPI schema"""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="TaskForge API",
        version="1.0.0",
        description="""
        TaskForge is a distributed job processing framework that provides:

        ## Core Features
        * 🚀 Type-safe job definitions using Pydantic
        * 💪 Reliable message processing with RabbitMQ
        * 📊 Job state persistence in PostgreSQL
        * 🔄 Automatic retries and error handling
        * 🎯 Priority-based job processing
        * 🔍 Job progress tracking
        * 📈 Prometheus metrics
        * 🔒 Concurrency control

        ## Job Processing Features
        * Scheduling and recurring jobs
        * Job dependencies and workflows
        * Job prioritization queues
        * Progress tracking and updates
        * Error handling and retries
        * Resource management

        ## Integration Features
        * Webhook delivery
        * Event system
        * External service integration
        * Metrics collection

        ## Security
        All API endpoints require authentication using an API key provided in
        the `X-API-Key` header.
        """,
        routes=app.routes,
    )

    # Add security scheme
    openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
    }

    # Apply security globally
    openapi_schema["security"] = [{"ApiKeyAuth": []}]

    # Add custom response schemas
    openapi_schema["components"]["schemas"].update(
        {
            "Error": {
                "type": "object",
                "properties": {
                    "detail": {"type": "string"},
                    "code": {"type": "string"},
                    "params": {"type": "object", "additionalProperties": True},
                },
                "required": ["detail", "code"],
            },
            "JobState": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": [
                            "pending",
                            "running",
                            "completed",
                            "failed",
                            "retrying",
                        ],
                    },
                    "progress": {"type": "number", "minimum": 0, "maximum": 100},
                    "result": {"type": "object", "additionalProperties": True},
                    "error": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "started_at": {"type": "string", "format": "date-time"},
                    "completed_at": {"type": "string", "format": "date-time"},
                },
                "required": ["id", "status", "created_at"],
            },
        }
    )

    # Add operation examples
    for path in openapi_schema["paths"].values():
        for operation in path.values():
            if "responses" in operation:
                operation["responses"]["400"] = {
                    "description": "Bad Request",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Error"},
                            "example": {
                                "detail": "Invalid input data",
                                "code": "validation_error",
                                "params": {"field": "value"},
                            },
                        }
                    },
                }
                operation["responses"]["401"] = {
                    "description": "Unauthorized",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Error"},
                            "example": {
                                "detail": "Invalid API key",
                                "code": "unauthorized",
                            },
                        }
                    },
                }

            # Add examples if not present
            if "requestBody" in operation:
                content = operation["requestBody"]["content"]
                if "application/json" in content:
                    schema = content["application/json"].get("schema", {})
                    if "example" not in content["application/json"]:
                        example = generate_example(schema)
                        if example:
                            content["application/json"]["example"] = example

    app.openapi_schema = openapi_schema
    return app.openapi_schema


def generate_example(schema: Dict[str, Any]) -> Any:
    """Generate example data from OpenAPI schema"""
    if "example" in schema:
        return schema["example"]

    if "type" not in schema:
        return None

    if schema["type"] == "object":
        if "properties" not in schema:
            return {}

        example = {}
        for prop, prop_schema in schema["properties"].items():
            example[prop] = generate_example(prop_schema)
        return example

    elif schema["type"] == "array":
        if "items" not in schema:
            return []

        return [generate_example(schema["items"])]

    elif schema["type"] == "string":
        if schema.get("format") == "date-time":
            return "2024-01-01T12:00:00Z"
        return "string"

    elif schema["type"] == "number":
        return 0.0

    elif schema["type"] == "integer":
        return 0

    elif schema["type"] == "boolean":
        return False

    return None


def export_openapi_docs(app: FastAPI, format: str = "json") -> str:
    """Export API documentation"""
    schema = custom_openapi(app)

    if format == "yaml":
        return yaml.dump(schema, sort_keys=False)
    return json.dumps(schema, indent=2)


def setup_api_docs(app: FastAPI) -> None:
    """Set up API documentation with custom templates and styling"""

    @app.get("/api/docs/spec.json", tags=["Documentation"])
    async def get_openapi_spec():
        """Get OpenAPI specification in JSON format"""
        return custom_openapi(app)

    @app.get("/api/docs/spec.yaml", tags=["Documentation"])
    async def get_openapi_spec_yaml():
        """Get OpenAPI specification in YAML format"""
        from fastapi.responses import PlainTextResponse

        return PlainTextResponse(
            export_openapi_docs(app, format="yaml"), media_type="text/yaml"
        )

    # Custom HTML template for API docs
    DOCS_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>TaskForge API Documentation</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.min.css" />
        <style>
            body { margin: 0; padding: 0; }
            .swagger-ui .topbar { display: none; }
            .swagger-ui .info { margin: 20px 0; }
            .swagger-ui .info .title { font-size: 36px; }
            .swagger-ui .scheme-container { box-shadow: none; }
        </style>
    </head>
    <body>
        <div id="swagger-ui"></div>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-bundle.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-standalone-preset.min.js"></script>
        <script>
            window.onload = function() {
                window.ui = SwaggerUIBundle({
                    url: '/api/docs/spec.json',
                    dom_id: '#swagger-ui',
                    deepLinking: true,
                    presets: [
                        SwaggerUIBundle.presets.apis,
                        SwaggerUIStandalonePreset
                    ],
                    plugins: [
                        SwaggerUIBundle.plugins.DownloadUrl
                    ],
                    layout: "BaseLayout",
                    defaultModelsExpandDepth: 1,
                    defaultModelExpandDepth: 1,
                    defaultModelRendering: 'model',
                    displayRequestDuration: true,
                    docExpansion: 'list',
                    filter: true,
                    showExtensions: true,
                    showCommonExtensions: true,
                    tryItOutEnabled: true
                });
            };
        </script>
    </body>
    </html>
    """

    @app.get("/api/docs/custom", include_in_schema=False)
    async def custom_docs():
        """Custom API documentation page"""
        from fastapi.responses import HTMLResponse

        return HTMLResponse(DOCS_TEMPLATE)
