* When sending null in ServiceMessage.data - now it's throwing BadRequest, but before it was
  security-exception. Must be changed back to how it was.
* ServicePrincipal - as a second constructor parameter must expect Collection of permissions.
* ServiceRoleDefinition - as a second constructor parameter must expect Collection of permissions.
* VaultServiceRolesInstaller:
  * must validate for non-null constructor parameters (check other security classes as well).
  * ".key" change to ".identity-key"
* Add debug logs for Auth/Authz error situations.
* In ServiceMethodInvoker - wrap principalMapper.map(context) into Mono.defer()
