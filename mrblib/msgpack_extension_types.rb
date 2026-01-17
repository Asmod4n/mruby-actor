unless MessagePack.ext_packer_registered? (Exception)
  MessagePack.register_pack_type(10, Exception) do |exc|
    [ exc.class.name, exc.message, exc.backtrace ].to_msgpack
  end

  MessagePack.register_unpack_type(10) do |data|
    data = MessagePack.unpack(data)
    exc_class = data[0].constantize
    exc = data[1] ? exc_class.new(data[1]) : exc_class.new
    exc.set_backtrace(data[2]) if data[2]
    exc
  end
end

unless MessagePack.ext_packer_registered? (Proc)
  MessagePack.register_pack_type(11, Proc) do |prc|
    prc.to_irep
  end

  MessagePack.register_unpack_type(11) do |data|
    Proc.from_irep(data)
  end
end

unless MessagePack.ext_packer_registered? (Class)
  MessagePack.register_pack_type(12, Class) do |klass|
    klass.name
  end

  MessagePack.register_unpack_type(12) do |data|
    data.constantize
  end
end
