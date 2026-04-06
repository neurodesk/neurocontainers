# system-wide profile.modules                                          #
# Initialize modules for all sh-derivative shells                      #
#----------------------------------------------------------------------#
trap "" 1 2 3

if [ -d /usr/share/lmod/lmod/init ]; then
    lmod_init_root=/usr/share/lmod/lmod/init
elif [ -d /usr/share/lmod/6.6/init ]; then
    lmod_init_root=/usr/share/lmod/6.6/init
else
    echo "Unable to locate lmod init scripts." >&2
    trap - 1 2 3
    return 1 2>/dev/null || exit 1
fi

case "$0" in
    -bash|bash|*/bash) . "${lmod_init_root}/bash" ;;
       -ksh|ksh|*/ksh) . "${lmod_init_root}/ksh" ;;
       -zsh|zsh|*/zsh) . "${lmod_init_root}/zsh" ;;
          -sh|sh|*/sh) . "${lmod_init_root}/sh" ;;
                    *) . "${lmod_init_root}/sh" ;;  # default for scripts
esac

unset lmod_init_root

trap - 1 2 3
