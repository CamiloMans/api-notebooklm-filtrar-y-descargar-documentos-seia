"""Notebook auth propagation tests for api_app."""

from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

import api_app


def _encode_auth_header(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _headers(include_notebook_auth: bool = True) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_app.API_BEARER_TOKEN or 'change_me'}",
    }
    if include_notebook_auth:
        headers[api_app.NOTEBOOK_AUTH_HEADER] = _encode_auth_header(
            {
                "version": 1,
                "cookies": {"SID": "sid-base"},
                "cookie_names": ["SID"],
                "cookie_domains": [".google.com"],
            }
        )
    return headers


def _document_row(document_id: str = "doc-1") -> dict:
    return {
        "id": document_id,
        "selected": True,
        "seleccionar": True,
        "nombre_archivo": "archivo.pdf",
        "nombre_archivo_final": "archivo_final.pdf",
        "extension": ".pdf",
        "ruta_relativa": "Linea_base/archivo_final.pdf",
        "tamano_bytes": 123,
        "nivel_descarga_descompresion": 0,
        "origen": "descarga",
        "categoria": "Linea Base",
        "texto_link": "Capitulo 1",
        "url_origen": "https://seia.example/archivo.pdf",
    }


class _FakeTable:
    def __init__(self, table_name: str, failed_rows: list[dict]):
        self.table_name = table_name
        self.failed_rows = failed_rows
        self.mode = "select"

    def select(self, *_args, **_kwargs):
        self.mode = "select"
        return self

    def update(self, *_args, **_kwargs):
        self.mode = "update"
        return self

    def insert(self, *_args, **_kwargs):
        self.mode = "insert"
        return self

    def delete(self, *_args, **_kwargs):
        self.mode = "delete"
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def execute(self):
        if self.mode == "select" and self.table_name == "adenda_document_files":
            return SimpleNamespace(data=self.failed_rows)
        return SimpleNamespace(data=[])


class _FakeSupabase:
    def __init__(self, failed_rows: list[dict] | None = None):
        self.failed_rows = failed_rows or []

    def table(self, table_name: str) -> _FakeTable:
        return _FakeTable(table_name, self.failed_rows)


class ApiAppNotebookAuthTests(unittest.TestCase):
    def setUp(self):
        api_app.API_BEARER_TOKEN = api_app.API_BEARER_TOKEN or "change_me"
        self.client = TestClient(api_app.app)

    def test_create_selection_with_new_notebook_propagates_header_auth(self):
        process_upload = MagicMock(return_value=None)

        with patch.object(api_app, "get_supabase_client", return_value=object()), patch.object(
            api_app,
            "load_run",
            return_value={"tipo": "ifa", "id_documento": "123", "nombre_notebooklm": ""},
        ), patch.object(
            api_app,
            "load_selected_documents",
            return_value=[_document_row()],
        ), patch.object(
            api_app,
            "list_notebook_sources",
            return_value=[],
        ), patch.object(
            api_app,
            "queue_notebook_upload_selection",
            return_value=None,
        ), patch.object(
            api_app,
            "process_notebook_upload_background",
            process_upload,
        ):
            response = self.client.post(
                "/api/v1/adenda/crear-y-cargar-notebook-filtrado",
                json={
                    "run_id": "run-1",
                    "nombre_notebook": "Notebook usuario",
                    "selected_document_ids": ["doc-1"],
                },
                headers=_headers(),
            )

        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload["status"], "upload_queued")
        self.assertEqual(process_upload.call_args.args[4]["cookies"]["SID"], "sid-base")

    def test_create_selection_with_existing_notebook_keeps_notebook_id_and_auth(self):
        process_upload = MagicMock(return_value=None)

        with patch.object(api_app, "get_supabase_client", return_value=object()), patch.object(
            api_app,
            "load_run",
            return_value={"tipo": "ifa", "id_documento": "123", "nombre_notebooklm": ""},
        ), patch.object(
            api_app,
            "load_selected_documents",
            return_value=[_document_row()],
        ), patch.object(
            api_app,
            "list_notebook_sources",
            return_value=[],
        ), patch.object(
            api_app,
            "queue_notebook_upload_selection",
            return_value=None,
        ), patch.object(
            api_app,
            "process_notebook_upload_background",
            process_upload,
        ):
            response = self.client.post(
                "/api/v1/adenda/crear-y-cargar-notebook-filtrado",
                json={
                    "run_id": "run-1",
                    "notebook_id": "nb-123",
                    "selected_document_ids": ["doc-1"],
                },
                headers=_headers(),
            )

        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload["notebooklm_id"], "nb-123")
        self.assertEqual(process_upload.call_args.args[4]["cookies"]["SID"], "sid-base")

    def test_create_selection_rejects_more_documents_than_notebook_capacity(self):
        process_upload = MagicMock(return_value=None)

        with patch.object(api_app, "NOTEBOOK_SOURCES_PER_NOTEBOOK", 2), patch.object(
            api_app,
            "get_supabase_client",
            return_value=object(),
        ), patch.object(
            api_app,
            "load_run",
            return_value={"tipo": "ifa", "id_documento": "123", "nombre_notebooklm": ""},
        ), patch.object(
            api_app,
            "load_selected_documents",
            return_value=[
                _document_row("doc-1"),
                _document_row("doc-2"),
                _document_row("doc-3"),
            ],
        ), patch.object(
            api_app,
            "queue_notebook_upload_selection",
            return_value=None,
        ) as queue_upload, patch.object(
            api_app,
            "process_notebook_upload_background",
            process_upload,
        ):
            response = self.client.post(
                "/api/v1/adenda/crear-y-cargar-notebook-filtrado",
                json={
                    "run_id": "run-1",
                    "nombre_notebook": "Notebook usuario",
                    "selected_document_ids": ["doc-1", "doc-2", "doc-3"],
                },
                headers=_headers(),
            )

        self.assertEqual(response.status_code, 422)
        self.assertIn("2 fuente", response.json()["detail"])
        queue_upload.assert_not_called()
        process_upload.assert_not_called()

    def test_create_selection_counts_existing_notebook_sources(self):
        process_upload = MagicMock(return_value=None)

        with patch.object(api_app, "NOTEBOOK_SOURCES_PER_NOTEBOOK", 2), patch.object(
            api_app,
            "get_supabase_client",
            return_value=object(),
        ), patch.object(
            api_app,
            "load_run",
            return_value={"tipo": "ifa", "id_documento": "123", "nombre_notebooklm": ""},
        ), patch.object(
            api_app,
            "load_selected_documents",
            return_value=[_document_row("doc-1"), _document_row("doc-2")],
        ), patch.object(
            api_app,
            "list_notebook_sources",
            return_value=[{"id": "source-1"}],
        ) as list_sources, patch.object(
            api_app,
            "queue_notebook_upload_selection",
            return_value=None,
        ) as queue_upload, patch.object(
            api_app,
            "process_notebook_upload_background",
            process_upload,
        ):
            response = self.client.post(
                "/api/v1/adenda/crear-y-cargar-notebook-filtrado",
                json={
                    "run_id": "run-1",
                    "notebook_id": "nb-123",
                    "selected_document_ids": ["doc-1", "doc-2"],
                },
                headers=_headers(),
            )

        self.assertEqual(response.status_code, 422)
        self.assertIn("quedan 1 cupo", response.json()["detail"])
        list_sources.assert_called_once()
        queue_upload.assert_not_called()
        process_upload.assert_not_called()

    def test_selected_documents_zip_export_can_be_downloaded_in_parts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_path = Path(tmp_dir) / "Linea_base" / "archivo_final.pdf"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_bytes(b"documento de prueba para exportacion por partes")

            row = _document_row("doc-1")
            row["ruta_relativa"] = "Linea_base/archivo_final.pdf"
            run = {
                "id": "run-1",
                "tipo": "ifa",
                "output_dir": tmp_dir,
            }

            with patch.object(api_app, "ZIP_EXPORT_PART_SIZE_BYTES", 64), patch.object(
                api_app,
                "get_supabase_client",
                return_value=object(),
            ), patch.object(
                api_app,
                "load_run",
                return_value=run,
            ), patch.object(
                api_app,
                "load_selected_documents",
                return_value=[row],
            ):
                export_response = self.client.post(
                    "/api/v1/adenda/descarga-documentos-seia/run-1/documentos-seleccionados/export",
                    json={"selected_document_ids": ["doc-1"]},
                    headers=_headers(include_notebook_auth=False),
                )
                self.assertEqual(export_response.status_code, 200)
                export_payload = export_response.json()
                self.assertEqual(export_payload["part_size_bytes"], 64)
                self.assertGreater(export_payload["size_bytes"], 0)
                self.assertGreaterEqual(export_payload["parts"], 1)

                part_response = self.client.get(
                    (
                        "/api/v1/adenda/descarga-documentos-seia/run-1/"
                        f"documentos-seleccionados/export/{export_payload['export_id']}/part/0"
                    ),
                    headers=_headers(include_notebook_auth=False),
                )

            self.assertEqual(part_response.status_code, 200)
            self.assertGreater(len(part_response.content), 0)
            self.assertEqual(part_response.headers["x-zip-export-id"], export_payload["export_id"])
            self.assertTrue(part_response.headers["content-range"].startswith("bytes 0-"))

    def test_retry_upload_forwards_notebook_auth_to_upload_helper(self):
        fake_supabase = _FakeSupabase(failed_rows=[_document_row()])
        auth_seed = {"cookies": {"SID": "sid-base"}, "csrf_token": "csrf", "session_id": "sess"}

        with patch.object(api_app, "get_supabase_client", return_value=fake_supabase), patch.object(
            api_app,
            "load_run",
            return_value={
                "id": "run-1",
                "tipo": "ifa",
                "output_dir": "C:\\temp",
                "notebooklm_id": "nb-123",
                "metadata": {},
            },
        ), patch.object(
            api_app,
            "set_run_retry_attempts",
            return_value=1,
        ), patch.object(
            api_app,
            "prepare_notebook_client_seed",
            return_value=auth_seed,
        ), patch.object(
            api_app,
            "list_notebook_sources",
            return_value=[],
        ), patch.object(
            api_app,
            "upload_documents_batch_and_single",
            return_value={
                "uploaded_ok": 1,
                "uploaded_failed": 0,
                "items": [{"document_id": "doc-1", "uploaded": True}],
            },
        ) as upload_documents, patch.object(
            api_app,
            "load_run_documents",
            return_value=[],
        ), patch.object(
            api_app,
            "retryable_document_ids",
            return_value=[],
        ), patch.object(
            api_app,
            "mark_remaining_documents_for_retry",
            return_value=[],
        ), patch.object(
            api_app,
            "update_document_upload_state",
            return_value=None,
        ):
            response = self.client.post(
                "/api/v1/adenda/reintentar-carga-notebook",
                json={"run_id": "run-1"},
                headers=_headers(),
            )

        self.assertEqual(response.status_code, 200)
        kwargs = upload_documents.call_args.kwargs
        self.assertEqual(kwargs["notebook_auth"]["cookies"]["SID"], "sid-base")
        self.assertEqual(kwargs["auth_seed"], auth_seed)

    def test_retry_upload_rejects_when_notebook_has_no_capacity(self):
        fake_supabase = _FakeSupabase(failed_rows=[_document_row()])
        auth_seed = {"cookies": {"SID": "sid-base"}, "csrf_token": "csrf", "session_id": "sess"}

        with patch.object(api_app, "NOTEBOOK_SOURCES_PER_NOTEBOOK", 1), patch.object(
            api_app,
            "get_supabase_client",
            return_value=fake_supabase,
        ), patch.object(
            api_app,
            "load_run",
            return_value={
                "id": "run-1",
                "tipo": "ifa",
                "output_dir": "C:\\temp",
                "notebooklm_id": "nb-123",
                "metadata": {},
            },
        ), patch.object(
            api_app,
            "prepare_notebook_client_seed",
            return_value=auth_seed,
        ), patch.object(
            api_app,
            "list_notebook_sources",
            return_value=[{"id": "source-1"}],
        ), patch.object(
            api_app,
            "set_run_retry_attempts",
            return_value=1,
        ) as set_retry_attempts, patch.object(
            api_app,
            "upload_documents_batch_and_single",
            return_value={},
        ) as upload_documents:
            response = self.client.post(
                "/api/v1/adenda/reintentar-carga-notebook",
                json={"run_id": "run-1"},
                headers=_headers(),
            )

        self.assertEqual(response.status_code, 422)
        self.assertIn("quedan 0 cupo", response.json()["detail"])
        set_retry_attempts.assert_not_called()
        upload_documents.assert_not_called()

    def test_legacy_endpoint_keeps_working_without_notebook_auth_header(self):
        with patch.object(api_app, "get_supabase_client", return_value=object()), patch.object(
            api_app,
            "adenda_exists",
            return_value=True,
        ), patch.object(
            api_app,
            "run_seia_notebook_pipeline",
            return_value={
                "status": "success",
                "id_documento": "123",
                "notebooklm_id": "nb-123",
                "nombre_notebooklm": "Notebook legacy",
                "documents_found": 1,
                "documents_uploaded_ok": 1,
                "documents_uploaded_failed": 0,
                "output_dir": "C:\\temp",
                "elapsed_seconds": 1.5,
            },
        ) as run_pipeline:
            response = self.client.post(
                "/api/v1/adendas/notebooklm",
                json={
                    "documento_seia": "https://seia.sea.gob.cl/documentos/documento.php?idDocumento=123",
                    "id_adenda": 99,
                },
                headers=_headers(include_notebook_auth=False),
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(run_pipeline.call_args.kwargs["notebook_auth"])


if __name__ == "__main__":
    unittest.main()
