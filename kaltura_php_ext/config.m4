PHP_ARG_ENABLE(kaltura, whether to Kaltura support,[ --enable-kaltura   Enable Kaltura support])
if test "$PHP_KALTURA" = "yes"; then
  AC_DEFINE(HAVE_KALTURA, 1, [Whether you have Kaltura support])
  PHP_NEW_EXTENSION(kaltura, kaltura.c, $ext_shared)
fi
