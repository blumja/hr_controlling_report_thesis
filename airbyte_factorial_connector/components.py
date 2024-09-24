#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import (
    DeclarativeAuthenticator,
)
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    ParentStreamConfig,
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.declarative.transformations.transformation import (
    RecordTransformation,
)
from airbyte_cdk.sources.declarative.types import (
    Config,
    Record,
    StreamSlice,
    StreamState,
)
from airbyte_cdk.sources.declarative.auth.oauth import DeclarativeSingleUseRefreshTokenOauth2Authenticator
from airbyte_cdk.sources.declarative.auth.token import BearerAuthenticator


@dataclass
class FactorialOauth(DeclarativeAuthenticator):
    """
    Factorial's access tokens expires after an hour while the refresh token is
    valid for a week. Since refreshing the access token via the refresh token
    automatically issues a new refresh token, making it single use, this class
    provides a mechanism to deal with the constantly changing refresh token.
    """
    config: Mapping[str, Any]
    token_auth: BearerAuthenticator
    oauth2: DeclarativeSingleUseRefreshTokenOauth2Authenticator

    def __new__(cls, token_auth, oauth2, config, *args, **kwargs):
        return token_auth if config["credentials"]["auth_type"] == "access_token" else oauth2


@dataclass
class UpdatesSubstreamPartitionRouter(SubstreamPartitionRouter):
    """
    The contracts stream provides the compensation stream with the needed
    compensation_ids to be included in the request url path but since the
    data isn't just a singular value but an empty or integer filled array,
    a custom partition router is required to retrieve every id individually
    and also skip empty values.
    """

    parent_stream_configs: List[ParentStreamConfig]

    def stream_slices(self) -> Iterable[StreamSlice]:
        if not self.parent_stream_configs:
            yield from []

        for parent_stream_config in self.parent_stream_configs:
            yield from self._generate_slices_from_parent_stream(
                parent_stream_config
            )

    def _generate_slices_from_parent_stream(
        self, parent_stream_config
    ) -> Iterable[StreamSlice]:
        parent_stream = parent_stream_config.stream
        parent_field = parent_stream_config.parent_key

        for parent_stream_slice in parent_stream.stream_slices(
            sync_mode=SyncMode,
            cursor_field=None,
            stream_state=StreamState,
        ):
            yield from self._process_parent_stream_slice(
                parent_stream, parent_field, parent_stream_slice
            )

    def _process_parent_stream_slice(
        self, parent_stream, parent_field, parent_stream_slice
    ) -> Iterable[Dict[str, Any]]:
        for parent_record in parent_stream.read_records(
            sync_mode=SyncMode.full_refresh,
            cursor_field=None,
            stream_slice=parent_stream_slice,
            stream_state=None,
        ):
            if parent_record.get(parent_field.string):
                stream_state_values = parent_record.get(parent_field.string)
                yield from self._generate_compensation_ids(stream_state_values)

    def _generate_compensation_ids(
        self, stream_state_values
    ) -> Iterable[Dict[str, Any]]:
        for stream_state_value in stream_state_values:
            yield StreamSlice(partition={"compensation_id": stream_state_value}, cursor_slice={})


@dataclass
class CustomTablesValueTransformation(RecordTransformation):
    """
    The data from the custom_tables_val stream is dynamic and the columns can't
    be defined in the schema. Since multiple custom tables and their
    corresponding values are retrieved in the same stream a custom
    transformation has to be used to get them into a standard format that stays
    the same regardless of the customized columns. It simply takes all of the
    columns except for id and employee_id and dumps them in the new col_val
    column.

    Initial schema:
    "data": {
        "id": 37486,
        "employee_id": 1339560,
        "608865": "Ausbildungsbezeichnung",
        "608866": "2023-06-24",
        "608867": "2023-06-30",
        "608868": "In Progress",
    }

    After custom transformation:
    "data": {
        "id": 37486,
        "employee_id": 1339560,
        "col_val": {
            "608865": "Ausbildungsbezeichnung",
            "608866": "2023-06-24",
            "608867": "2023-06-30",
            "608868": "In Progress",
        }
    }
    """

    def transform(
        self,
        record: Record,
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> Record:
        record["col_val"] = {
            k: v for k, v in record.items() if k not in ["id", "employee_id"]
        }

        for key in list(record["col_val"]):
            del record[key]

        return record
