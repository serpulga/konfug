# -*- coding: utf-8 -*-

import abc
import os
import json
import functools

import firebase_admin
from google.cloud import datastore
from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError


DEFAULT_FALSEY_EXPRESSIONS = ('0', 'false', 0, False, None,)
UTF8 = 'UTF-8'


class KonfugError(Exception):
    pass


class KonfugMissingError(KonfugError):
    def __init__(self, missing_setting, is_secret=False):
        self._missing_setting = missing_setting
        space = "setting" if is_secret is False else "secret"
        self.message = f'Missing {space} "{missing_setting}"'

        super(KonfugMissingError, self).__init__(self.message)


class KonfugMetaConfigError(KonfugError):
    def __init__(self, global_name, kwarg_name):
        self._global_name = global_name
        self._kwarg_name = kwarg_name
        self.message = f'Missing {global_name} env or {kwarg_name} kwarg'

        super(KonfugMetaConfigError, self).__init__(self.message)


class HotFugError(KonfugError):
    def __init__(self, message):
        self.message = message

        super(HotFugError, self).__init__(self.message)


class HotFug(abc.ABC):

    __metaclass__ = abc.ABCMeta

    _FIREBASE = None
    COLLECTION = None
    FIREBASE_CREDENTIAL_JSON = None

    def __init__(self, key, default_val=None, apply_=None):
        self._key = key
        self._default_val = default_val
        self._apply = apply_ if apply_ else lambda v: v

    @classmethod
    def _firebase(cls):
        if not cls._FIREBASE:
            try:
                firebase_admin.get_app()
            except ValueError:
                cred = firebase_admin.credentials.Certificate(
                    cls.FIREBASE_CREDENTIAL_JSON
                )
                try:
                    firebase_admin.initialize_app(cred)
                except ValueError:
                    pass
                except firebase_admin.exceptions.FirebaseError as err:
                    raise HotFugError("Could not initialize firebase") from err

            # Caching Firebase client at class level.
            cls._FIREBASE = (
                firebase_admin.firestore.client().collection(cls.COLLECTION)
            )
        return cls._FIREBASE

    def retrieve(self):
        doc = type(self)._firebase().document(self._key).get()
        if doc.exists:
            val = self._apply(doc.to_dict().get('value'))
        elif self._default_val:
            val = self._default_val
        else:
            val = None
        return val

    @classmethod
    def hotfug_cls(cls, credentials, collection):
        return type(
            f'_{cls.__name__}',
            (cls,),
            {
                'FIREBASE_CREDENTIAL_JSON': credentials,
                'COLLECTION': collection
            }
        )


class HotFugCollection(object):
    def __init__(self):
        self._hotconfs = []

    def __getattribute__(self, attribute):
        value = super(HotFugCollection, self).__getattribute__(attribute)

        if issubclass(type(value), (HotFug,)):
            value = value.retrieve()
            if value is None:
                if attribute in self._hotconfs:
                    value = self._hotconfs[attribute]
                else:
                    raise KonfugMissingError(attribute)

        return value

    def __setattr__(self, name, value):
        if issubclass(type(value), (HotFug,)):
            self._hotconfs.append(name)
        super(HotFugCollection, self).__setattr__(name, value)


class Konfug(object):
    def __init__(self, **kwargs):
        try:
            cls = type(self)
            project_id = cls.check_metaconfig(
                kwargs, 'GOOGLE_CLOUD_PROJECT', 'project_id'
            )
            settings_kind = cls.check_metaconfig(
                kwargs, 'KONFUG_DATASTORE_SETTINGS_KIND', 'settings_kind'
            )
            namespace = cls.check_metaconfig(
                kwargs, 'KONFUG_DATASTORE_NAMESPACE', 'namespace'
            )
            common_namespace = cls.check_metaconfig(
                kwargs,
                'KONFUG_DATASTORE_COMMON_NAMESPACE',
                'common_namespace',
                required=False
            )
            stringlist_separator = cls.check_metaconfig(
                kwargs,
                'KONFUG_STRINGLIST_SEPARATOR',
                'stringlist_separator',
                required=False
            )
            self._stringlist_separator = stringlist_separator or ','

            self._falsey_expressions = kwargs.get(
                'falsey_expressions', DEFAULT_FALSEY_EXPRESSIONS
            )
            self._to_bool = functools.partial(
                type(self).to_bool, falsey_expressions=self._falsey_expressions
            )
            self._to_stringlist = functools.partial(
                type(self).to_stringlist, sep=self._stringlist_separator
            )
            self._to_int = int
            self._to_dict = type(self).to_dict
            self._to_float = float

            force_datastore = cls.to_bool(
                cls.check_metaconfig(
                    kwargs,
                    'KONFUG_FORCE_DATASTORE',
                    'force_datastore',
                    required=False
                ),
                falsey_expressions=self._falsey_expressions
            )

            self._secret_resource_name_tpl = (
                f"projects/{project_id}/"
                f"secrets/{{secret_id}}/"
                f"versions/latest"
            )

            self._skip_datastore = kwargs.get('skip_datastore', False)
            self._skip_secret_manager = kwargs.get(
                'skip_secret_manager', False
            )
            self._skip_firebase = kwargs.get('skip_firebase', False)
            self._hotcollection = HotFugCollection()
            self._hot_cls = HotFug

            self._dataclient = None
            self._secretclient = None

            if not self._skip_datastore:
                self._dataclient = datastore.Client(project=project_id)
            else:
                self._dataclient = None

            if not self._skip_secret_manager:
                self._secretclient = secretmanager.SecretManagerServiceClient()
            else:
                self._secretclient = None

            def fetch_kinds(ns):
                kinds = {}
                if self._skip_datastore is False:
                    kinds = self._dataclient.query(
                        kind=settings_kind, namespace=ns
                    ).fetch()
                    kinds = dict(next(iter(kinds)))
                return kinds

            self._settings = fetch_kinds(namespace)
            if common_namespace:
                self._common_settings = fetch_kinds(common_namespace)
            else:
                self._common_settings = {}
        except (StopIteration, TypeError, DefaultCredentialsError):
            if force_datastore:
                raise
            # Defaults to empty dictionary. Settings will be taken from the
            # environment variables.
            self._common_settings = {}
        else:
            self._common_settings.update(self._settings)

    @staticmethod
    def check_metaconfig(kwargs, global_name, kwarg_name, required=True):
        if kwargs.get(kwarg_name):
            return kwargs[kwarg_name]
        elif os.environ.get(global_name):
            return os.environ[global_name]
        if required:
            raise KonfugMetaConfigError(global_name, kwarg_name)

    def raw_setting(
        self, key, default_val=None, apply_=None, nullable=False, hot=False
    ):
        if hot and not self._skip_firebase:
            return self._hot_cls(key, default_val=default_val, apply_=apply_)

        if key in os.environ:
            val = os.getenv(key)
        elif key in self._common_settings:
            val = self._common_settings[key]
        elif default_val is not None:
            val = default_val
        elif not nullable:
            raise KonfugMissingError(key)
        else:
            val = None
        return apply_(val) if callable(apply_) else val

    def string(self, key, default_val=None, hot=False):
        return self.raw_setting(key, default_val=default_val, hot=hot)

    def flag(self, key, default_val=None, hot=False):
        return self.raw_setting(
            key, default_val=default_val, apply_=self._to_bool, hot=hot
        )

    def stringlist(self, key, default_val=None, hot=False):
        return self.raw_setting(
            key, default_val=default_val, apply_=self._to_stringlist, hot=hot
        )

    def integer(self, key, default_val=None, hot=False):
        return self.raw_setting(
            key, default_val=default_val, apply_=self._to_int, hot=hot
        )

    def dictionary(self, key, default_val=None, hot=False):
        return self.raw_setting(
            key, default_val=default_val, apply_=self._to_dict, hot=hot
        )

    def floatnum(self, key, default_val=None, hot=False):
        return self.raw_setting(
            key, default_val=default_val, apply_=self._to_float, hot=hot
        )

    @staticmethod
    def to_bool(val, falsey_expressions=DEFAULT_FALSEY_EXPRESSIONS):
        return val not in falsey_expressions

    @staticmethod
    def to_stringlist(val, sep=','):
        return [v.strip() for v in val.split(sep) if v.strip()]

    @staticmethod
    def to_dict(val):
        try:
            # os.environ has a JSON formatted string.
            dict_ = json.loads(val)
        except TypeError:
            # Google Datastore should return a dictionary.
            dict_ = val

        if not isinstance(dict_, dict):
            raise ValueError(f'Not a dict {val}')
        else:
            return dict_

    def secret(self, key, default_val=None, transform=None, encoding=UTF8):
        if key in os.environ:
            val = os.getenv(key)
        elif self._skip_secret_manager or self._secretclient is None:
            val = None
        else:
            name = self._secret_resource_name_tpl.format(secret_id=key)
            secret = self._secretclient.access_secret_version(
                                                    request={"name": name})
            val = secret.payload.data.decode(encoding)
            if transform and callable(transform):
                val = transform(val)

        if val is None and default_val:
            val = default_val
        elif val is None:
            raise KonfugMissingError(key, is_secret=True)

        return val

    def hot_settings(self, credentials=None, collection=None):
        self._hot_cls = HotFug.hotfug_cls(credentials, collection)
        return self._hotcollection
