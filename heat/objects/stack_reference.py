#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""StackReference object"""

from oslo_log import log as logging
from oslo_versionedobjects import base
from oslo_versionedobjects import fields

from heat.common import exception
from heat.db.sqlalchemy import api as db_api
from heat.objects import base as heat_base
from heat.objects import resource as resource_objects
from heat.objects import stack as stack_object

LOG = logging.getLogger(__name__)


class StackReference(
        heat_base.HeatObject,
        base.VersionedObjectDictCompat,
        base.ComparableVersionedObject,
):
    fields = {
        'id': fields.IntegerField(),
        'stack_id': fields.StringField(),
        'rsrc_id': fields.StringField(),
        'reference_stack_id': fields.StringField(),
        'reference_rsrc_id': fields.StringField(),
        'created_at': fields.DateTimeField(read_only=True),
        'updated_at': fields.DateTimeField(nullable=True),
    }

    @staticmethod
    def _from_db_object(reference, db_reference):
        """Method to help with migration to objects.

        Converts a database entity to a formal object.
        """
        if db_reference is None:
            return None
        for field in reference.fields:
            reference[field] = db_reference[field]
        reference.obj_reset_changes()
        return reference

    @staticmethod
    def _from_object_to_dict(reference):
        result = {}
        for field in reference.fields:
            result[field] = reference[field]
        return result

    @classmethod
    def get(cls, context, reference_id):
        return cls._from_db_object(
            cls(), db_api.stack_reference_get(context, reference_id))

    @classmethod
    def get_all_by_stack(cls, context, stack_id):
        return [
            cls._from_db_object(
                cls(), ref
            ) for ref in db_api.stack_reference_get_all_by_stack(
                context, stack_id)
        ]

    @classmethod
    def get_all_by_rsrc(cls, context, stack_id, rsrc_id):
        return [
            cls._from_db_object(
                cls(), ref
            ) for ref in db_api.stack_reference_get_all_by_rsrc(
                context, stack_id, rsrc_id)
        ]

    @classmethod
    def get_all_by_reference_stack(cls, context, reference_stack_id):
        return [
            cls._from_db_object(
                cls(), ref
            ) for ref in db_api.stack_reference_get_all_by_reference_stack(
                context, reference_stack_id)
        ]

    @classmethod
    def set(cls, context, values):
        if len(
            cls.get_all_by_rsrc(context, values['stack_id'], values['rsrc_id'])
        ) == 0:
            return cls._from_db_object(
                cls(), db_api.stack_reference_set(context, values))

    @classmethod
    def delete(cls, context, reference_id):
        db_api.stack_reference_delete(context, reference_id)

    @classmethod
    def delete_all_by_stack(cls, context, stack_id):
        db_api.stack_reference_delete_all_by_stack(
            context, stack_id)

    @classmethod
    def delete_all_by_reference_stack(cls, context, reference_stack_id):
        db_api.stack_reference_delete_all_by_reference_stack(
            context, reference_stack_id)

    @classmethod
    def delete_all_by_rsrc(cls, context, stack_id, rsrc_id):
        db_api.stack_reference_delete_all_by_rsrc(
            context, stack_id, rsrc_id)

    @classmethod
    def validate_no_match_reference(cls, context, stack_id, action):
        reference = cls.get_all_by_reference_stack(
            context, stack_id)
        if len(reference) > 0:
            reference = [cls._from_object_to_dict(ref) for ref in reference]
            raise exception.ActionNotSupportedWithReference(
                action=action, reference=reference)

    @classmethod
    def set_stack_reference(cls, context, external_id, stack_id, rsrc_id):
        # Check external resource is create by heat or not. If yes,
        # adding stack reference.
        rs = resource_objects.Resource.get_all_by_physical_resource_id(
            context, external_id)
        if len(rs) > 0:
            rsrc = rs[0]
            if rsrc.status == 'COMPLETE' and (
                rsrc.action not in ['DELETE', 'INIT']
            ):
                values = {
                    'stack_id': stack_id,
                    'rsrc_id': rsrc_id,
                    'reference_stack_id': rsrc.stack_id,
                    'reference_rsrc_id': rsrc.id}
                if rsrc_id == rsrc.id:
                    # A Stack resource, we need to check status with Stack obj

                    stack_obj = stack_object.Stack.get_by_id(
                        context, rsrc.physical_resource_id)
                    if not stack_obj:
                        raise exception.ExternalRsrcNotReady(
                            reference_rsrc_id=rsrc.physical_resource_id,
                            reference_stack_id=rsrc.physical_resource_id,
                            action='Unknown action', status='Unknown statue')
                    elif stack_obj.status != 'COMPLETE' or (
                        stack_obj.action in ['DELETE', 'INIT']
                    ):
                        raise exception.ExternalRsrcNotReady(
                            reference_rsrc_id=stack_obj.id,
                            reference_stack_id=stack_obj.id,
                            action=stack_obj.action, status=stack_obj.status)

                    values['reference_stack_id'] = rsrc.physical_resource_id

                LOG.info('Creating Stack reference with values %s',
                         values)
                ref = cls.set(context, values=values)
                return ref
            else:
                raise exception.ExternalRsrcNotReady(
                    reference_rsrc_id=rsrc.id,
                    reference_stack_id=rsrc.stack_id,
                    action=rsrc.action, status=rsrc.status)
