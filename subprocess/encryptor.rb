require "openssl"
require "base64"

def encrypt(data)
  begin
    cipher = OpenSSL::Cipher.new("AES-128-CBC")
    cipher.encrypt
    # cipher.key = "5268ce1d16f45b92e4bb156bf8978404b5a4e5d244e030fc4b6d51eb3c4eb636" # Convierte la clave hexadecimal en una secuencia de bytes
    cipher.key = "5268ce1d16f45b92e4bb156bf8978404" # Convierte la clave hexadecimal en una secuencia de bytes

    crypt = cipher.update(data) + cipher.final
    crypt_string = (Base64.encode64(crypt))
    crypt_string = crypt_string.gsub("+", "-").gsub("/", "_")
    return crypt_string
  rescue Exception => e
    return e
  end
end

if ARGV.empty?
  puts "Usage: ruby script.rb arg1 arg2 ..."
  exit(1)
end
  
  # Iterar a través de los argumentos
ARGV.each_with_index do |arg, index|
  puts encrypt(arg)
end
