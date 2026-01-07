unless MessagePack.ext_packer_registered? (Exception)
  MessagePack.register_pack_type(10, Exception) do |exc|
    [ exc.class.name, exc.message, exc.backtrace ].to_msgpack
  end
end
unless MessagePack.ext_unpacker_registered? (10)
  MessagePack.register_unpack_type(10) do |data|
    data = MessagePack.unpack(data)
    exc_class = data[0].constantize
    exc = exc_class.new(data[1])
    exc.set_backtrace(data[2])
    exc
  end
end

unless MessagePack.ext_packer_registered? (Proc)
  MessagePack.register_pack_type(11, Proc) do |prc|
    prc.to_irep
  end
end
unless MessagePack.ext_unpacker_registered? (11)
  MessagePack.register_unpack_type(11) do |data|
    Proc.from_irep(data)
  end
end
