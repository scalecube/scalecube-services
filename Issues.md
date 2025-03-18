* When sending null in ServiceMessage.data - now it's throwing BadRequest, but before it was
  security-exception. Must be changed back to how it was.
* Add debug logs for Auth/Authz error situations.
