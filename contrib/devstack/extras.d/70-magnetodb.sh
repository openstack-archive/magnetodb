if is_service_enabled magnetodb; then
    if [[ "$1" == "source" ]]; then
        # Initial source
        source $TOP_DIR/lib/magnetodb
    elif [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo_summary "Installing Magnetodb"
        install_magnetodb
    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        # no-op
        :
    elif [[ "$1" == "stack" && "$2" == "extra" ]]; then
        # no-op
        :
    fi

    if [[ "$1" == "unstack" ]]; then
	echo_summary "Stopping Magnetodb"
        stop_magnetodb
    fi

    if [[ "$1" == "clean" ]]; then
        # no-op
        :
    fi
fi
