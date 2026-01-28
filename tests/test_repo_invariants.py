"""Static tests for repository invariants."""

import inspect
from uuid import UUID

import pytest

from metismedia.db.repos.base import BaseRepo
from metismedia.db.types import TenantId


class TestRepoInvariants:
    """Test that repository classes enforce tenant_id invariants."""

    def test_base_repo_has_tenant_id_in_all_methods(self) -> None:
        """Test that all BaseRepo abstract methods require tenant_id."""
        # Known abstract methods that should require tenant_id
        abstract_methods = ["get_by_id", "create", "update", "delete"]

        # Check each abstract method has tenant_id as first parameter
        for method_name in abstract_methods:
            method = getattr(BaseRepo, method_name)
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())

            # Skip 'self' parameter
            if params and params[0] == "self":
                params = params[1:]

            assert len(params) > 0, f"{method_name} must have at least one parameter"
            first_param = params[0]
            assert (
                first_param == "tenant_id"
            ), f"{method_name} must have 'tenant_id' as first parameter (after self), got: {first_param}"

    def test_base_repo_method_signatures(self) -> None:
        """Test specific BaseRepo method signatures include tenant_id."""
        # Test get_by_id
        sig = inspect.signature(BaseRepo.get_by_id)
        params = list(sig.parameters.keys())
        assert params[0] == "self"
        assert params[1] == "tenant_id"
        assert params[2] == "entity_id"

        # Test create
        sig = inspect.signature(BaseRepo.create)
        params = list(sig.parameters.keys())
        assert params[0] == "self"
        assert params[1] == "tenant_id"
        assert params[2] == "data"

        # Test update
        sig = inspect.signature(BaseRepo.update)
        params = list(sig.parameters.keys())
        assert params[0] == "self"
        assert params[1] == "tenant_id"
        assert params[2] == "entity_id"
        assert params[3] == "data"

        # Test delete
        sig = inspect.signature(BaseRepo.delete)
        params = list(sig.parameters.keys())
        assert params[0] == "self"
        assert params[1] == "tenant_id"
        assert params[2] == "entity_id"

    def test_base_repo_helper_methods(self) -> None:
        """Test that helper methods (now, generate_uuid) don't require tenant_id."""
        # now() should not require tenant_id
        sig = inspect.signature(BaseRepo.now)
        params = list(sig.parameters.keys())
        assert "tenant_id" not in params

        # generate_uuid() should not require tenant_id
        sig = inspect.signature(BaseRepo.generate_uuid)
        params = list(sig.parameters.keys())
        assert "tenant_id" not in params

    def test_base_repo_is_abstract(self) -> None:
        """Test that BaseRepo is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            BaseRepo()  # type: ignore

    def test_concrete_repo_must_implement_all_methods(self) -> None:
        """Test that a concrete repo must implement all abstract methods."""

        class IncompleteRepo(BaseRepo):
            """Incomplete repo missing implementations."""

            def get_by_id(self, tenant_id: TenantId, entity_id: UUID) -> None:  # type: ignore
                """Only implements one method."""
                pass

        # Should raise TypeError when trying to instantiate (missing create, update, delete)
        with pytest.raises(TypeError):
            IncompleteRepo()  # type: ignore

    def test_tenant_id_type_alias(self) -> None:
        """Test that TenantId type alias is properly defined."""
        from uuid import uuid4

        # TenantId should be a NewType
        assert TenantId is not None
        # Should be able to create a TenantId from UUID
        uuid_val = uuid4()
        tenant_id = TenantId(uuid_val)
        assert tenant_id == uuid_val
