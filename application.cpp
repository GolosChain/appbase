#include <appbase/application.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/thread/thread_pool.hpp>
#include <iostream>
#include <fstream>

namespace appbase {

    namespace bpo = boost::program_options;
    namespace asio = boost::asio;
    using boost::asio::ip::tcp;
    using bpo::options_description;
    using bpo::variables_map;
    using std::cout;

    class impl {
    public:
        impl():_cli_options("Application Options"){
            io_serv.reset(new asio::io_service());
        }

        std::vector<tcp::endpoint> resolve( const std::string& host, uint16_t port ) {
            tcp::resolver res( *io_serv );

            tcp::resolver::iterator it = res.resolve( tcp::resolver::query(host, std::to_string(uint64_t(port))) );
            tcp::resolver::iterator et;

            std::vector<tcp::endpoint> eps;
            for (; et != it; ++it) {
                if (it->endpoint().address().is_v4()) {
                    eps.push_back( it->endpoint() );
                }
            }
            return eps;
        }

        map< string, std::shared_ptr< abstract_plugin > >  plugins; ///< all registered plugins
        vector< abstract_plugin* >                         initialized_plugins; ///< stored in the order they were started running
        vector< abstract_plugin* >                         running_plugins; ///< stored in the order they were started running
        std::unique_ptr< asio::io_service >                io_serv;
        std::string                                        version_info;

        const variables_map*                               _options = nullptr;
        options_description                                _cli_options;
        options_description                                _cfg_options;
        bfs::path                                          _cfg_path;
        variables_map                                      _args;
        boost::thread_group                                thread_pool;
        bfs::path                                          _data_dir;
    };


    boost::thread_group& application::scheduler(){
        return my->thread_pool;
    }

    boost::asio::io_service& application::get_io_service() {
        return *my->io_serv;
    }

    void application::plugin_initialized( abstract_plugin& plug ) {
        my->initialized_plugins.push_back( &plug );
    }

    void application::plugin_started( abstract_plugin& plug ) {
        my->running_plugins.push_back( &plug );
    }

    void application::register_plugin(const std::string& name, std::shared_ptr<abstract_plugin> plug) {
        my->plugins[name] = plug;
    }

    void application::set_version_string( const string& version ) {
        my->version_info = version;
    }

    application::application() :my(new impl()){
    }

    application::~application() { }

    void application::startup() {
        for (const auto& plugin : my->initialized_plugins) {
            plugin->startup();
        }
    }

    application& application::instance( bool reset ) {
        static application* _app = new application();
        if (reset) {
            delete _app;
            _app = new application();
        }
        return *_app;
    }
    application& app() { return application::instance(); }
    application& reset() { return application::instance( true ); }


    void application::set_program_options() {
       options_description app_cfg_opts( "Application Config Options" );
       options_description app_cli_opts( "Application Command Line Options" );
       app_cfg_opts.add_options()
               ("plugin", bpo::value< vector<string> >()->composing(), "Plugin(s) to enable, may be specified multiple times");

       app_cli_opts.add_options()
                ("help,h", "Print this help message and exit.")
                ("version,v", "Print version information.")
                ("data-dir,d", bpo::value<bfs::path>()->default_value( "witness_node_data_dir" ), "Directory containing configuration file config.ini")
                ("config,c", bpo::value<bfs::path>()->default_value( "config.ini" ), "Configuration file name relative to data-dir");

        my->_cli_options.add(app_cli_opts);
        my->_cli_options.add(app_cfg_opts); // is it needed to show "--plugin" in --help?
        my->_cfg_options.add(app_cfg_opts);

        for (auto& plug : my->plugins) {
            boost::program_options::options_description cli("Command Line Options for " + plug.second->get_name());
            boost::program_options::options_description cfg("Config Options for " + plug.second->get_name());
            plug.second->set_program_options(cli, cfg);
            if (cfg.options().size())
                my->_cfg_options.add(cfg);
            if (cli.options().size())
                my->_cli_options.add(cli);
        }
    }

    bool application::initialize_impl(int argc, char** argv, vector<abstract_plugin*> autostart_plugins) {
        try {
            set_program_options();
            bpo::options_description all_options;
            all_options.add(my->_cli_options).add(my->_cfg_options);
            bpo::store(bpo::parse_command_line(argc, argv, all_options), my->_args);

            if (my->_args.count("help")) {
                cout << my->_cli_options << "\n";
                return false;
            }

            if (my->_args.count("version")) {
                cout << my->version_info << "\n";
                return false;
            }

            bfs::path data_dir = my->_args["data-dir"].as<bfs::path>();
            if (data_dir.is_relative())
                data_dir = bfs::current_path() / data_dir;
            my->_data_dir = data_dir;

            bfs::path config_file_name = my->_args["config"].as<bfs::path>();
            if (config_file_name.is_relative())
                config_file_name = data_dir / config_file_name;
            if (!bfs::exists(config_file_name)) {
                write_default_config(config_file_name);
            }

            my->_cfg_path = config_file_name.make_preferred();
            bpo::store(bpo::parse_config_file<char>(my->_cfg_path.string().c_str(), my->_cfg_options, true), my->_args);

            if (my->_args.count("plugin") > 0) {
                auto plugins = my->_args.at("plugin").as<std::vector<std::string>>();
                for (auto& arg : plugins) {
                    vector<string> names;
                    boost::split(names, arg, boost::is_any_of(" \t,"));
                    for(const std::string& name : names) {
                        get_plugin(name).initialize(my->_args);
                    }
                }
            }
            for (const auto& plugin : autostart_plugins) {
                if (plugin != nullptr && plugin->get_state() == abstract_plugin::registered) {
                    plugin->initialize(my->_args);
                }
            }
            bpo::notify(my->_args);

            return true;
        }
        catch (const boost::program_options::error& e) {
            std::cerr << "Error parsing command line: " << e.what() << "\n";
            return false;
        }
    }

    void application::shutdown() {
        for (auto ritr = my->running_plugins.rbegin();
            ritr != my->running_plugins.rend(); ++ritr) {
            (*ritr)->shutdown();
        }
        for (auto ritr = my->running_plugins.rbegin();
            ritr != my->running_plugins.rend(); ++ritr) {
            my->plugins.erase((*ritr)->get_name());
        }
        my->running_plugins.clear();
        my->initialized_plugins.clear();
        my->plugins.clear();
    }

    void application::quit() {
        my->io_serv->stop();
    }

    void application::exec() {
        std::shared_ptr<boost::asio::signal_set> sigint_set(new boost::asio::signal_set(*my->io_serv, SIGINT));
        sigint_set->async_wait([sigint_set,this](const boost::system::error_code& err, int num) {
            quit();
            sigint_set->cancel();
        });

        std::shared_ptr<boost::asio::signal_set> sigterm_set(new boost::asio::signal_set(*my->io_serv, SIGTERM));
        sigterm_set->async_wait([sigterm_set,this](const boost::system::error_code& err, int num) {
            quit();
            sigterm_set->cancel();
        });

        my->io_serv->run();

        shutdown(); /// perform synchronous shutdown
    }

    void application::write_default_config(const bfs::path& cfg_file) {
        if (!bfs::exists(cfg_file.parent_path()))
            bfs::create_directories(cfg_file.parent_path());

        std::ofstream out_cfg(bfs::path(cfg_file).make_preferred().string());
        std::stringstream delayed;  // delay sections (should be printed after normal options)
        string cur_section = "";

        for (const boost::shared_ptr<bpo::option_description> od : my->_cfg_options.options()) {
            std::stringstream out;

            if (!od->description().empty())
                out << "# " << od->description() << "\n";

            auto name = od->long_name();
            auto pos = name.rfind('.');
            auto new_section = false;
            auto is_section = pos != std::string::npos;
            if (is_section) {
                auto section = name.substr(0, pos);
                name.erase(0, pos + 1);
                new_section = cur_section.compare(section) != 0;
                if (new_section) {
                    cur_section = section;
                    out << "[" << section << "]\n";
                }
            }

            boost::any store;
            if (name.length() == 0) {
                // It's section description, skip value output
            } else if (!od->semantic()->apply_default(store)) {
                out << "# " << name << " = \n";
            } else {
                auto example = od->format_parameter();
                if (example.empty())
                    // This is a boolean switch
                    out << name << " = " << "false\n";
                else {
                    // The string is formatted "arg (=<interesting part>)"
                    example.erase(0, 6);
                    example.erase(example.length()-1);
                    out << name << " = " << example << "\n";
                }
            }

            if (is_section)
                delayed << (new_section ? "\n" : "") << out.str();
            else
                out_cfg << "\n" << out.str();
        }
        out_cfg << delayed.str() << "\n";
        out_cfg.close();
    }

    abstract_plugin* application::find_plugin( const string& name )const {
        auto itr = my->plugins.find( name );

        if (itr == my->plugins.end()) {
            return nullptr;
        }

        return itr->second.get();
    }

    abstract_plugin& application::get_plugin(const string& name)const {
        auto ptr = find_plugin(name);
        if (!ptr)
            BOOST_THROW_EXCEPTION(std::runtime_error("unable to find plugin: " + name));
        return *ptr;
    }

    bfs::path application::data_dir() const {
        return my->_data_dir;
    }
    const bfs::path& application::config_path() const {
        return my->_cfg_path;
    }

    void application::add_program_options(const options_description& cli, const options_description& cfg) {
        my->_cli_options.add(cli);
        my->_cfg_options.add(cfg);
    }

    const variables_map& application::get_args() const {
        return my->_args;
    }

    std::vector<tcp::endpoint> application::resolve_string_to_ip_endpoints(const std::string &endpoint_string) {
        string::size_type colon_pos = endpoint_string.find(':');
        if (colon_pos == std::string::npos)
            BOOST_THROW_EXCEPTION(std::runtime_error("Missing required port number in endpoint string \"" + endpoint_string + "\""));

        std::string port_string = endpoint_string.substr(colon_pos + 1);

        try {
            uint16_t port = boost::lexical_cast<uint16_t>(port_string);
            std::string hostname = endpoint_string.substr(0, colon_pos);
            auto endpoints = my->resolve(hostname, port);

            if (endpoints.empty())
                BOOST_THROW_EXCEPTION(std::runtime_error("The host name can not be resolved: " + hostname));

            return endpoints;
        } catch (const boost::bad_lexical_cast &) {
            BOOST_THROW_EXCEPTION(std::runtime_error("Bad port: " + port_string));
        }
    }
} /// namespace appbase