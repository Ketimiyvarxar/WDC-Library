from wdc import dco, dbc
import pytest
import warnings
warnings.filterwarnings("ignore")

# this tests initialization of dbc() instance
class Test_init_dbc():
    # initialize dbc() instance correctly by passing a string
    def test_init_correctly(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        assert isinstance(my_dbc.server_url, str)

    # initialize dbc() instance by passing not a string
    def test_init_not_string(self):
        with pytest.raises(TypeError):
            my_dbc = dbc(2)

# this tests send_query()
class Test_send_query():
    # send incorrect query
    def test_send_wrong_query(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        with pytest.raises(Exception):
            my_dbc.send_query("for $c in")

    # send correct query
    def test_send_correct_query(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        response = my_dbc.send_query('for $c in (AvgLandTemp) return 1')
        assert (response.status_code == 200) and (response.content == b'1')

    # pass to the method a variable, which is not a string
    def test_send_not_str(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        with pytest.raises(TypeError):
            my_dbc.send_query(1)

# this tests initialization of dco() instance
class Test_init_dco():
    # init by not passing a dbc() instance
    def test_not_correct_dbc(self):
        with pytest.raises(TypeError):
            my_dco = dco(2)

    # init correct dbc() instance
    def test_pass_dbc(self):
        my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
        my_dco = dco(my_dbc)
        assert isinstance(my_dco.DBC, dbc)

# we will get coverages from the https://ows.rasdaman.org/rasdaman/ows
def create_dco():
    my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
    my_dco = dco(my_dbc)
    return my_dco

# this tests initialization of the variable in the dco()
class Test_init_var():
    # init_var gets a string in a correct format
    def test_good_format(self):
        my_dco = create_dco()
        assert isinstance(my_dco.initialize_var("$c in (AvgLandTemp)"), dco)
    
    # init_var gets not a string
    def test_type_error(self):
        my_dco = create_dco()
        with pytest.raises(TypeError):
            my_dco.initialize_var(42)
    
    # init_var gets a string in not correct format
    def test_format_error(self):
        my_dco = create_dco()
        with pytest.raises(ValueError):
            my_dco.initialize_var("$cin(AvgLandTemp)")


# this is a dco instance with a good initialized variable
def create_good_dco():
    my_dbc = dbc("https://ows.rasdaman.org/rasdaman/ows")
    my_dco = dco(my_dbc)
    return my_dco.initialize_var("$c in (AvgLandTemp)")

# this tests subset()
class Test_subset():
    # pass correct values as a subset
    def test_correct_subset(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.subset(var_name = '$c', subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")'), dco)
    
    # don't pass one of the arguments
    def test_dont_pass_one_arg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.subset(var_name = '$c')

    # pass as an argument not a string to the var_name argument
    def test_pass_not_str_varname(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.subset(var_name = 2, subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")')
    
    # pass as an argument not a string to the subset argument
    def test_pass_not_str_subset(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.subset(var_name = '$c', subset = 1000)

    # pass as an argument not existing variable
    def test_pass_non_existing_var(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.subset(var_name = '$t', subset = 'Lat(53.08), Long(8.80), ansi("2014-01":"2014-12")')

# this tests set_format() method
class Test_set_format():
    # pass an existing format(PNG)
    def test_correct_format_png(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.set_format('PNG'), dco)

    # pass an existing format(CSV)
    def test_correct_format_csv(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.set_format('CSV'), dco)
    
    # pass an existing format(JPEG)
    def test_correct_format_jpeg(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.set_format('JPEG'), dco)

    # pass nothing
    def test_pass_nothing(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.set_format()

    # pass non-string argument
    def test_pass_non_string(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.set_format(2)

    # pass non-existing format
    def test_pass_non_existing_format(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.set_format('TIFF')

# this tests where() method
class Test_where():
    # pass an argument with existing var. name and in a string format
    def test_pass_correct(self):
        my_dco = create_good_dco()
        assert isinstance(my_dco.where('$c > 2'), dco)

    # pass nothing
    def test_pass_nothing(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.where()
    
    # pass a filter condition with non-existing variable
    def test_pass_non_existing_var(self):
        my_dco = create_good_dco()
        with pytest.raises(ValueError):
            my_dco.where("$t > 10")
    
    # pass non-string argument
    def pass_non_string_arg(self):
        my_dco = create_good_dco()
        with pytest.raises(TypeError):
            my_dco.where(2)
