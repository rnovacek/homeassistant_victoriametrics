# VictoriaMetrics integration for Home Assistant

The Home Assitant VictoriaMetrics integration records all events and state changes and feeds the data to a VictoriaMetrics instance.

## Installation

1. Enable Graphite interface on your VictoriaMetrics instance by using `-graphiteListenAddr=:2003` command line option
2. Copy `custom_components/victoriametrics` directory into your `custom_components` in your configuration directory.
3. Add this to your `configuration.yaml` file:

```
graphite:
```

4. Restart Home Assistant

## Configuration variables

**host** string (optional, default: localhost)
IP address of your victoriametrics host, e.g., 192.168.1.10.

**port** integer (optional, default: 2003)
Graphite port on victoriametrics host.

**protocol** string (optional, default: tcp)
Type of communication protocol: tcp or udp.

**prefix** string (optional, default: ha)
Prefix is the metric prefix in victoriametrics.
