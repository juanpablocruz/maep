package gui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
	"github.com/juanpablocruz/maep/internal/maep"
)

type state int

const (
	statusNormal state = iota
	stateDone
)

type newOperationScreen struct {
	err    error
	form   *huh.Form
	lg     *lipgloss.Renderer
	styles *Styles
	state  state
	width  int
}

func initialNewOperationScreen() newOperationScreen {
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewInput().
				Title("Query").
				Description("Enter the query to be executed").
				Key("query"),

			huh.NewInput().
				Title("Arguments").
				Description("Enter the arguments to be passed to the query").
				Key("arguments"),
		),
	).
		WithWidth(45)

	m := newOperationScreen{
		form:  form,
		width: maxWidth,
	}

	m.lg = lipgloss.DefaultRenderer()
	m.styles = NewStyles(m.lg)

	return m
}

func (n newOperationScreen) Init() tea.Cmd {
	return nil
}

func (n newOperationScreen) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		n.width = min(msg.Width, maxWidth) - n.styles.Base.GetHorizontalFrameSize()
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "ctrl+c", "q":
			return n, tea.Quit
		}
	}

	var cmds []tea.Cmd

	form, cmd := n.form.Update(msg)
	if f, ok := form.(*huh.Form); ok {
		n.form = f
		cmds = append(cmds, cmd)
	}

	if n.form.State == huh.StateCompleted {

		query := n.form.GetString("query")
		args := n.form.GetString("arguments")
		op := maep.NewOperation([]byte(query), strings.Split(args, ","))

		return RootScreen().SwitchScreen(initialModel(&op))
	}

	return n, tea.Batch(cmds...)
}

func (n newOperationScreen) View() string {
	s := n.styles
	header := RootScreen().appBoundaryView("Welcome to MAEP client")
	body := lipgloss.JoinHorizontal(lipgloss.Top, n.form.View())
	return s.Base.Render(header + "\n" + body + "\n\n")
}
