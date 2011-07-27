import collectd

def cephtool_read(data=None):
    vl = collectd.Values(type='gauge')
    vl.plugin='python.cephtool'
    vl.dispatch(values=[random.random() * 100])

collectd.register_read(cephtool_read)
