actions :restart

#attribute :username, :kind_of => String, :default => nil

def initialize( *args )
  super
  @action = :restart
end
