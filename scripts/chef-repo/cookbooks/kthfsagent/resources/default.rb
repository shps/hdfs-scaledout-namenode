actions :init, :start, :stop, :restart

attribute :mgm_server, :kind_of => String, :name_attribute => true
attribute :restype, :kind_of => String, :required => true
attribute :enabled, :equal_to => [true, false, 'true', 'false'], :default => nil

attribute :echo, :kind_of => [TrueClass, FalseClass], :default => true
attribute :username, :kind_of => String, :default => nil

def initialize( *args )
  super
  @action = :create
end

